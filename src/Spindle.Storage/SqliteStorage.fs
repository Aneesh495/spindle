namespace Spindle.Storage

open System
open System.IO
open Microsoft.Data.Sqlite
open Spindle.Core.Domain
open Spindle.Core.Events
open Spindle.Core.Codecs
open Spindle.Engine.StateMachine

/// Embedded SQLite storage engine for durable workflow event sourcing, snapshots, and leases.
type SqliteStorage(connectionString: string) =

    let syncRoot = Object()

    let initializeDatabase () =
        use conn = new SqliteConnection(connectionString)
        conn.Open()
        use cmd = conn.CreateCommand()
        cmd.CommandText <- """
            CREATE TABLE IF NOT EXISTS workflow_events (
                workflow_id TEXT NOT NULL,
                run_id TEXT NOT NULL,
                sequence_number INTEGER NOT NULL,
                timestamp_ticks INTEGER NOT NULL,
                prev_hash INTEGER NOT NULL,
                record_hash INTEGER NOT NULL,
                event_json TEXT NOT NULL,
                PRIMARY KEY (workflow_id, run_id, sequence_number)
            );

            CREATE TABLE IF NOT EXISTS workflow_snapshots (
                workflow_id TEXT NOT NULL,
                run_id TEXT NOT NULL,
                sequence_number INTEGER NOT NULL,
                status TEXT NOT NULL,
                snapshot_json TEXT NOT NULL,
                PRIMARY KEY (workflow_id, run_id)
            );

            CREATE TABLE IF NOT EXISTS shard_leases (
                shard_id INTEGER PRIMARY KEY,
                node_address TEXT NOT NULL,
                fencing_token INTEGER NOT NULL,
                expires_at_ticks INTEGER NOT NULL
            );
        """
        cmd.ExecuteNonQuery() |> ignore

    do initializeDatabase ()

    interface IEventStore with
        member _.AppendRecords(wfId, runId, expectedSeq, records) = async {
            return lock syncRoot (fun () ->
                use conn = new SqliteConnection(connectionString)
                conn.Open()
                use tx = conn.BeginTransaction()

                // Check expected sequence number
                use checkCmd = conn.CreateCommand()
                checkCmd.Transaction <- tx
                checkCmd.CommandText <- """
                    SELECT COALESCE(MAX(sequence_number), 0)
                    FROM workflow_events
                    WHERE workflow_id = @wfId AND run_id = @runId;
                """
                checkCmd.Parameters.AddWithValue("@wfId", wfId.Value) |> ignore
                checkCmd.Parameters.AddWithValue("@runId", runId.ToString()) |> ignore
                let currentMaxSeq = Convert.ToInt64(checkCmd.ExecuteScalar())

                if currentMaxSeq <> expectedSeq.Value then
                    tx.Rollback()
                    Error (StorageError.ConcurrencyConflict(expectedSeq, SequenceNumber currentMaxSeq))
                else
                    for recItem in records do
                        use insertCmd = conn.CreateCommand()
                        insertCmd.Transaction <- tx
                        insertCmd.CommandText <- """
                            INSERT INTO workflow_events (
                                workflow_id, run_id, sequence_number, timestamp_ticks,
                                prev_hash, record_hash, event_json
                            ) VALUES (
                                @wfId, @runId, @seq, @ticks, @prevHash, @recHash, @eventJson
                            );
                        """
                        insertCmd.Parameters.AddWithValue("@wfId", wfId.Value) |> ignore
                        insertCmd.Parameters.AddWithValue("@runId", runId.ToString()) |> ignore
                        insertCmd.Parameters.AddWithValue("@seq", recItem.SequenceNumber.Value) |> ignore
                        insertCmd.Parameters.AddWithValue("@ticks", recItem.Timestamp.Ticks) |> ignore
                        insertCmd.Parameters.AddWithValue("@prevHash", int64 recItem.PrevHash) |> ignore
                        insertCmd.Parameters.AddWithValue("@recHash", int64 recItem.RecordHash) |> ignore
                        insertCmd.Parameters.AddWithValue("@eventJson", Json.serialize recItem.Event) |> ignore
                        insertCmd.ExecuteNonQuery() |> ignore

                    tx.Commit()
                    Ok ()
            )
        }

        member _.ReadRecords(wfId, runId, fromSeq) = async {
            return lock syncRoot (fun () ->
                use conn = new SqliteConnection(connectionString)
                conn.Open()
                use cmd = conn.CreateCommand()
                cmd.CommandText <- """
                    SELECT sequence_number, timestamp_ticks, prev_hash, record_hash, event_json
                    FROM workflow_events
                    WHERE workflow_id = @wfId AND run_id = @runId AND sequence_number >= @fromSeq
                    ORDER BY sequence_number ASC;
                """
                cmd.Parameters.AddWithValue("@wfId", wfId.Value) |> ignore
                cmd.Parameters.AddWithValue("@runId", runId.ToString()) |> ignore
                cmd.Parameters.AddWithValue("@fromSeq", fromSeq.Value) |> ignore

                use reader = cmd.ExecuteReader()
                let result = ResizeArray<JournalRecord>()
                while reader.Read() do
                    let seq = SequenceNumber (reader.GetInt64(0))
                    let ticks = Timestamp (reader.GetInt64(1))
                    let prevHash = uint32 (reader.GetInt64(2))
                    let recHash = uint32 (reader.GetInt64(3))
                    let eventJson = reader.GetString(4)
                    let evt = Json.deserialize<WorkflowEvent>(eventJson)
                    result.Add {
                        SequenceNumber = seq
                        Timestamp = ticks
                        Event = evt
                        PrevHash = prevHash
                        RecordHash = recHash
                    }
                Seq.toList result
            )
        }

    interface ISnapshotStore with
        member _.SaveSnapshot(snapshot) = async {
            lock syncRoot (fun () ->
                use conn = new SqliteConnection(connectionString)
                conn.Open()
                use cmd = conn.CreateCommand()
                cmd.CommandText <- """
                    INSERT INTO workflow_snapshots (workflow_id, run_id, sequence_number, status, snapshot_json)
                    VALUES (@wfId, @runId, @seq, @status, @json)
                    ON CONFLICT(workflow_id, run_id) DO UPDATE SET
                        sequence_number = excluded.sequence_number,
                        status = excluded.status,
                        snapshot_json = excluded.snapshot_json;
                """
                cmd.Parameters.AddWithValue("@wfId", snapshot.WorkflowId.Value) |> ignore
                cmd.Parameters.AddWithValue("@runId", snapshot.RunId.ToString()) |> ignore
                cmd.Parameters.AddWithValue("@seq", snapshot.CurrentSequence.Value) |> ignore
                cmd.Parameters.AddWithValue("@status", snapshot.Status.ToString()) |> ignore
                cmd.Parameters.AddWithValue("@json", Json.serialize snapshot) |> ignore
                cmd.ExecuteNonQuery() |> ignore
            )
        }

        member _.LoadSnapshot(wfId, runId) = async {
            return lock syncRoot (fun () ->
                use conn = new SqliteConnection(connectionString)
                conn.Open()
                use cmd = conn.CreateCommand()
                cmd.CommandText <- """
                    SELECT snapshot_json FROM workflow_snapshots
                    WHERE workflow_id = @wfId AND run_id = @runId;
                """
                cmd.Parameters.AddWithValue("@wfId", wfId.Value) |> ignore
                cmd.Parameters.AddWithValue("@runId", runId.ToString()) |> ignore

                use reader = cmd.ExecuteReader()
                if reader.Read() then
                    let json = reader.GetString(0)
                    Some (Json.deserialize<WorkflowInstanceSnapshot> json)
                else
                    None
            )
        }

    interface IShardLeaseStore with
        member _.AcquireLease(shardId, nodeAddr, proposedToken, duration, now) = async {
            return lock syncRoot (fun () ->
                use conn = new SqliteConnection(connectionString)
                conn.Open()
                use tx = conn.BeginTransaction()

                use checkCmd = conn.CreateCommand()
                checkCmd.Transaction <- tx
                checkCmd.CommandText <- "SELECT fencing_token, expires_at_ticks FROM shard_leases WHERE shard_id = @sId;"
                checkCmd.Parameters.AddWithValue("@sId", shardId.Value) |> ignore
                use reader = checkCmd.ExecuteReader()

                let canAcquire =
                    if reader.Read() then
                        let currentExpires = reader.GetInt64(1)
                        now.Ticks >= currentExpires
                    else
                        true
                reader.Close()

                if not canAcquire then
                    tx.Rollback()
                    Error (sprintf "Shard %d lease is currently active" shardId.Value)
                else
                    let expiryTicks = now.AddMilliseconds(int64 duration.TotalMilliseconds).Ticks
                    use upsertCmd = conn.CreateCommand()
                    upsertCmd.Transaction <- tx
                    upsertCmd.CommandText <- """
                        INSERT INTO shard_leases (shard_id, node_address, fencing_token, expires_at_ticks)
                        VALUES (@sId, @addr, @token, @exp)
                        ON CONFLICT(shard_id) DO UPDATE SET
                            node_address = excluded.node_address,
                            fencing_token = excluded.fencing_token,
                            expires_at_ticks = excluded.expires_at_ticks;
                    """
                    upsertCmd.Parameters.AddWithValue("@sId", shardId.Value) |> ignore
                    upsertCmd.Parameters.AddWithValue("@addr", nodeAddr) |> ignore
                    upsertCmd.Parameters.AddWithValue("@token", proposedToken.Value) |> ignore
                    upsertCmd.Parameters.AddWithValue("@exp", expiryTicks) |> ignore
                    upsertCmd.ExecuteNonQuery() |> ignore
                    tx.Commit()
                    Ok proposedToken
            )
        }

        member _.RenewLease(shardId, token, duration, now) = async {
            return lock syncRoot (fun () ->
                use conn = new SqliteConnection(connectionString)
                conn.Open()
                use cmd = conn.CreateCommand()
                let expiryTicks = now.AddMilliseconds(int64 duration.TotalMilliseconds).Ticks
                cmd.CommandText <- """
                    UPDATE shard_leases
                    SET expires_at_ticks = @exp
                    WHERE shard_id = @sId AND fencing_token = @token;
                """
                cmd.Parameters.AddWithValue("@exp", expiryTicks) |> ignore
                cmd.Parameters.AddWithValue("@sId", shardId.Value) |> ignore
                cmd.Parameters.AddWithValue("@token", token.Value) |> ignore
                let rows = cmd.ExecuteNonQuery()
                rows > 0
            )
        }

        member _.ReleaseLease(shardId, token) = async {
            return lock syncRoot (fun () ->
                use conn = new SqliteConnection(connectionString)
                conn.Open()
                use cmd = conn.CreateCommand()
                cmd.CommandText <- "DELETE FROM shard_leases WHERE shard_id = @sId AND fencing_token = @token;"
                cmd.Parameters.AddWithValue("@sId", shardId.Value) |> ignore
                cmd.Parameters.AddWithValue("@token", token.Value) |> ignore
                let rows = cmd.ExecuteNonQuery()
                rows > 0
            )
        }
