namespace Spindle.Server

open System
open System.IO
open Microsoft.AspNetCore.Builder
open Microsoft.AspNetCore.Http
open Microsoft.Extensions.DependencyInjection
open Microsoft.Extensions.Hosting
open Spindle.Server.ServerTypes
open Spindle.Server.ApiHandlers

module Program =

    [<EntryPoint>]
    let main args =
        let builder = WebApplication.CreateBuilder(args)

        // Register application services
        builder.Services.AddSingleton<WorkflowRuntimeService>() |> ignore
        builder.Services.AddCors(fun options ->
            options.AddDefaultPolicy(fun policy ->
                policy.AllowAnyOrigin().AllowAnyHeader().AllowAnyMethod() |> ignore
            )
        ) |> ignore

        let app = builder.Build()
        app.UseCors() |> ignore

        // Health check endpoint
        app.MapGet("/healthz", Func<IResult>(fun () ->
            Results.Ok({| status = "ok"; service = "spindle-kernel" |})
        )) |> ignore

        // Start workflow execution endpoint
        app.MapPost("/api/v1/workflows/start", Func<StartWorkflowRequest, WorkflowRuntimeService, IResult>(fun req runtime ->
            let task = runtime.StartWorkflow(req) |> Async.StartAsTask
            task.Wait()
            match task.Result with
            | Ok summary -> Results.Created(sprintf "/api/v1/workflows/%s/status" summary.WorkflowId, summary)
            | Error err -> Results.BadRequest({| error = err |})
        )) |> ignore

        // Query workflow status endpoint
        app.MapGet("/api/v1/workflows/{id}/status", Func<string, WorkflowRuntimeService, IResult>(fun id runtime ->
            match runtime.GetWorkflowStatus(id) with
            | Some status -> Results.Ok(status)
            | None -> Results.NotFound({| error = sprintf "Workflow '%s' not found" id |})
        )) |> ignore

        // Query workflow event history endpoint
        app.MapGet("/api/v1/workflows/{id}/history", Func<string, WorkflowRuntimeService, IResult>(fun id runtime ->
            let task = runtime.GetWorkflowHistory(id) |> Async.StartAsTask
            task.Wait()
            match task.Result with
            | Some records ->
                let dtos = records |> List.map (fun r -> {|
                    sequenceNumber = r.SequenceNumber.Value
                    timestamp = r.Timestamp.ToString()
                    eventType = r.Event.GetType().Name
                    checksum = sprintf "0x%08X" r.RecordHash
                |})
                Results.Ok(dtos)
            | None -> Results.NotFound({| error = sprintf "History for workflow '%s' not found" id |})
        )) |> ignore

        // Signal workflow endpoint
        app.MapPost("/api/v1/workflows/{id}/signal", Func<string, SignalWorkflowRequest, WorkflowRuntimeService, IResult>(fun id req runtime ->
            let task = runtime.SignalWorkflow(id, req) |> Async.StartAsTask
            task.Wait()
            match task.Result with
            | Ok () -> Results.Accepted()
            | Error err -> Results.BadRequest({| error = err |})
        )) |> ignore

        // Query cluster topology endpoint
        app.MapGet("/api/v1/cluster/topology", Func<WorkflowRuntimeService, IResult>(fun runtime ->
            Results.Ok(runtime.GetClusterTopology())
        )) |> ignore

        // Engine telemetry metrics endpoint
        app.MapGet("/api/v1/metrics", Func<WorkflowRuntimeService, IResult>(fun runtime ->
            Results.Ok(runtime.GetMetrics())
        )) |> ignore

        printfn "Spindle Kernel Server listening on configured ports..."
        app.Run()
        0
