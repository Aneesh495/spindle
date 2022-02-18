.PHONY: all build test clean bench run-cli run-server verify

all: build test

build:
	dotnet build Spindle.sln -c Release

test:
	dotnet test Spindle.sln -c Release --verbosity normal

test-fast:
	dotnet test Spindle.sln -c Debug --no-build

verify: build test

clean:
	dotnet clean Spindle.sln
	rm -rf bin obj TestResults data wal storage

bench:
	dotnet run -c Release --project benchmarks/Spindle.Benchmarks

run-cli:
	dotnet run -c Release --project src/Spindle.Cli

run-server:
	dotnet run -c Release --project src/Spindle.Server
