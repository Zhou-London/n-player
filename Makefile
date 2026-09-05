# Builds and runs nplayer inside the dev container image. The repository is
# mounted at /work, so the submodule and the data directory are visible.

IMAGE ?= dev
DOCKER_RUN = docker run --rm -v "$(CURDIR):/work" -w /work $(IMAGE)
.PHONY: build run clean shell

build:
	$(DOCKER_RUN) bash -c 'cmake -S . -B build -G Ninja -DCMAKE_BUILD_TYPE=Release && cmake --build build'

# Runs a subcommand; ARGS defaults to converting data/input to data/output.
# Example: make run ARGS="convert --limit 1000000 --jobs 2".
ARGS ?= convert
run: build
	$(DOCKER_RUN) ./build/nplayer $(ARGS)

shell:
	docker run --rm -it -v "$(CURDIR):/work" -w /work $(IMAGE)

clean:
	rm -rf build
