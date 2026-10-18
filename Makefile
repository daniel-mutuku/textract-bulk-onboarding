LAMBDAS := unzip-plan unzip-chunk extract

# Lambda's provided.al2023 runtime runs an executable named "bootstrap".
#   GOOS/GOARCH         cross-compile for Linux on Graviton (arm64)
#   CGO_ENABLED=0       a fully static binary: nothing to link at runtime
#   -tags lambda.norpc  leave out the RPC mode only the retired go1.x runtime used
#   -trimpath, -buildvcs=false, -s -w
#                       a reproducible build: the same source gives the same
#                       bytes, so Terraform's source_code_hash only changes
#                       (and the function only redeploys) when the code does
GOBUILD := GOOS=linux GOARCH=arm64 CGO_ENABLED=0 go build -tags lambda.norpc -trimpath -buildvcs=false -ldflags='-s -w'

.PHONY: build test deploy clean

build: $(LAMBDAS:%=build/%/bootstrap)

build/%/bootstrap: FORCE
	$(GOBUILD) -o $@ ./cmd/lambda/$*

test:
	go test ./...

deploy: build
	terraform -chdir=infra apply

clean:
	rm -rf build

FORCE:
