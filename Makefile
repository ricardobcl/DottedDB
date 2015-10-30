REBAR3_URL=https://s3.amazonaws.com/rebar3/rebar3

# If there is a rebar in the current directory, use it
ifeq ($(wildcard rebar3),rebar3)
REBAR3 = $(CURDIR)/rebar3
endif

# Fallback to rebar on PATH
REBAR3 ?= $(shell test -e `which rebar3` 2>/dev/null && which rebar3 || echo "./rebar3")

# And finally, prep to download rebar if all else fails
ifeq ($(REBAR3),)
REBAR3 = $(CURDIR)/rebar3
endif

.PHONY: all

all: $(REBAR3)
	@$(REBAR3) do deps, compile

rel: compile
	@$(REBAR3) release -d false --overlay_vars config/vars.config

rel_dev: compile
	@$(REBAR3) release --overlay_vars config/vars.config

clean:
	@$(REBAR3) clean

compile:
	@$(REBAR3) compile

compile-no-deps:
	@$(REBAR3) compile

deps:
	@$(REBAR3) deps

doc: compile
	@$(REBAR3) edoc

dialyzer: compile
	@$(REBAR3) dialyzer

test: deps compile
	@$(REBAR3) do eunit skip_deps=true, ct, dialyzer


$(REBAR3):
	curl -Lo rebar3 $(REBAR3_URL) || wget $(REBAR3_URL)
	chmod a+x rebar3


##
## Developer targets
##
##  devN - Make a dev build for node N
##  stagedevN - Make a stage dev build for node N (symlink libraries)
##  devrel - Make a dev build for 1..$DEVNODES
##  stagedevrel Make a stagedev build for 1..$DEVNODES
##
##  Example, make a 68 node devrel cluster
##    make stagedevrel DEVNODES=68

.PHONY : devrel
DEVNODES ?= 4

# 'seq' is not available on all *BSD, so using an alternate in awk
SEQ = $(shell awk 'BEGIN { for (i = 1; i < '$(DEVNODES)'; i++) printf("%i ", i); print i ;exit(0);}')

$(eval stagedevrel : $(foreach n,$(SEQ),stagedev$(n)))
$(eval devrel : $(foreach n,$(SEQ),dev$(n)))

dev% : devclean all
	mkdir -p _build/dev/$@
	config/gen_dev $@ config/vars/dev_vars.config.src config/vars/$@_vars.config
	$(REBAR3) release -o _build/dev/$@ --overlay_vars config/vars/$@_vars.config

devclean: clean
	rm -rf _build/dev

