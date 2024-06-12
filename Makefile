BUILD     = build
EXE       = blueskybot
IMAGE     = flicknow/blueskybot
PORT      = 8777
RESTART   = always
TAG       = latest
VOLUME    = blueskybot-db
CONTAINER = blueskybot
DRYRUN    = 1
ENV       = .env
DEBUG     = 0
VARNISH_PORT = 8778
DOCKER_RUN = docker run --init --security-opt seccomp=unconfined --log-driver local --env-file $(ENV)

GOFILES = $(shell find blueskybot cmd pkg -name '*.go')

default: build

build: $(BUILD)/$(EXE)

test:
	go test $(shell test $(DEBUG) -ne 0 && echo -tags sqlite_trace) -v ./pkg/...

update:
	go get -u && go mod tidy

$(BUILD)/$(EXE): $(GOFILES) go.*
	go build $(shell test $(DEBUG) -ne 0 && echo -tags sqlite_trace) -o $(BUILD)/$(EXE) ./cmd/...

.dockerignore: .gitignore
	echo /.git       >  .dockerignore
	echo /.gitignore >> .dockerignore
	cat .gitignore   >> .dockerignore

image: .dockerignore
	docker build -t $(IMAGE):$(TAG) .

local-block: $(BUILD)/$(EXE)
	env $$(cat $(ENV) | xargs) $(BUILD)/$(EXE) block $(URL)

docker-block:
	docker run --security-opt seccomp=unconfined --rm --env-file $(ENV) --volume $(VOLUME):/var/db $(IMAGE):$(TAG) /$(EXE) block $(URL)

local-index: $(BUILD)/$(EXE)
	env $$(cat $(ENV) | xargs) $(BUILD)/$(EXE) index $(URL)

docker-index:
	docker run --security-opt seccomp=unconfined --rm --env-file $(ENV) --volume $(VOLUME):/var/db $(IMAGE):$(TAG) /$(EXE) index $(URL)

run: $(BUILD)/$(EXE)
	env $$(cat $(ENV) | xargs) $(BUILD)/$(EXE) run --dry-run

run-bot: $(BUILD)/$(EXE)
	env $$(cat $(ENV) | xargs) $(BUILD)/$(EXE) run --dry-run

run-server: $(BUILD)/$(EXE)
	env $$(cat $(ENV) | xargs) $(BUILD)/$(EXE) server $(shell test "$(PORT)" && echo '--listen=:$(PORT)')

lookup: $(BUILD)/$(EXE)
	env $$(cat $(ENV) | xargs) $(BUILD)/$(EXE) lookup $(URL)

migrate: $(BUILD)/$(EXE)
	env $$(cat $(ENV) | xargs) $(BUILD)/$(EXE) migrate

clean:
	rm -f $(BUILD)/$(EXE)

fmt:
	go fmt ./...

pull-image:
	docker pull $(IMAGE):$(TAG)

deploy: pull-image deploy-bot deploy-server

deploy-bot:
	-@docker stop $(CONTAINER)-bot && docker rm $(CONTAINER)-bot
	$(DOCKER_RUN) -d --name $(CONTAINER)-bot --restart $(RESTART) --publish 6060:6060 --volume $(VOLUME):/var/db $(IMAGE):$(TAG) /$(EXE) run $(shell test "$(DRYRUN)" && echo '--dry-run')

deploy-follow-indexer:
	-@docker stop $(CONTAINER)-follow-indexer && docker rm $(CONTAINER)-follow-indexer
	$(DOCKER_RUN) -d --name $(CONTAINER)-follow-indexer --restart $(RESTART) --volume $(VOLUME):/var/db $(IMAGE):$(TAG) /$(EXE) index-follows --daemon

deploy-server:
	-@docker stop $(CONTAINER)-server && docker rm $(CONTAINER)-server
	$(DOCKER_RUN) -d --name $(CONTAINER)-server --restart $(RESTART) --publish $(PORT):8080 --volume $(VOLUME):/var/db $(IMAGE):$(TAG) /$(EXE) server

deploy-varnish:
	-@docker stop $(CONTAINER)-varnish && docker rm $(CONTAINER)-varnish
	$(DOCKER_RUN) -d --name $(CONTAINER)-varnish --add-host host.docker.internal:host-gateway --restart $(RESTART) --publish $(VARNISH_PORT):80 --volume $(PWD)/varnish/default.vcl:/etc/varnish/default.vcl:ro varnish:alpine

