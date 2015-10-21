REBAR = $(shell pwd)/rebar
.PHONY: deps

all: deps compile

compile:
	$(REBAR) compile

deps:
	$(REBAR) get-deps

compile-no-deps:
	$(REBAR) compile skip_deps=true

test: compile
	$(REBAR) eunit skip_deps=true

clean:
	$(REBAR) clean

distclean: clean devclean relclean
	$(REBAR) delete-deps

rel: all
	$(REBAR) generate

relclean:
	rm -rf rel/dotted_db

xref: all
	$(REBAR) skip_deps=true xref

stage: rel
	$(foreach dep,$(wildcard deps/*), rm -rf rel/dotted_db/lib/$(shell basename $(dep))-* && ln -sf $(abspath $(dep)) rel/dotted_db/lib;)
	$(foreach app,$(wildcard apps/*), rm -rf rel/dotted_db/lib/$(shell basename $(app))-* && ln -sf $(abspath $(app)) rel/dotted_db/lib;)

##
## Lock Targets
##
##  see https://github.com/seth/rebar_lock_deps_plugin
lock: deps compile
	./rebar lock-deps

locked-all: locked-deps compile

locked-deps:
	@echo "Using rebar.config.lock file to fetch dependencies"
	./rebar -C rebar.config.lock get-deps


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

.PHONY : stagedevrel devrel
DEVNODES ?= 4

# 'seq' is not available on all *BSD, so using an alternate in awk
SEQ = $(shell awk 'BEGIN { for (i = 1; i < '$(DEVNODES)'; i++) printf("%i ", i); print i ;exit(0);}')

$(eval stagedevrel : $(foreach n,$(SEQ),stagedev$(n)))
$(eval devrel : $(foreach n,$(SEQ),dev$(n)))

dev% : all
	mkdir -p dev
	rel/gen_dev $@ rel/vars/dev_vars.config.src rel/vars/$@_vars.config
	(cd rel && $(REBAR) generate target_dir=../dev/$@ overlay_vars=vars/$@_vars.config)

stagedev% : dev%
	  $(foreach dep,$(wildcard deps/*), rm -rf dev/$^/lib/$(shell basename $(dep))* && ln -sf $(abspath $(dep)) dev/$^/lib;)
	  $(foreach app,$(wildcard apps/*), rm -rf dev/$^/lib/$(shell basename $(app))* && ln -sf $(abspath $(app)) dev/$^/lib;)

devclean: clean
	rm -rf dev


## Dialyzer

DIALYZER_APPS = kernel stdlib sasl erts ssl tools os_mon runtime_tools crypto inets \
	xmerl webtool eunit syntax_tools compiler public_key snmp


PLT ?= $(PWD)/.combo_dialyzer_plt
LOCAL_PLT = $(PWD)/.local_dialyzer_plt
DIALYZER_FLAGS ?= -Wunmatched_returns -Werror_handling -Wrace_conditions -Wunderspecs

${PLT}: compile-no-deps
	@if [ -f $(PLT) ]; then \
		dialyzer --check_plt --plt $(PLT) --apps $(DIALYZER_APPS) && \
		dialyzer --add_to_plt --plt $(PLT) --output_plt $(PLT) --apps $(DIALYZER_APPS) ; test $$? -ne 1; \
	else \
		dialyzer --build_plt --output_plt $(PLT) --apps $(DIALYZER_APPS); test $$? -ne 1; \
	fi

${LOCAL_PLT}: compile-no-deps
	@if [ -d deps ]; then \
		if [ -f $(LOCAL_PLT) ]; then \
			dialyzer --check_plt --plt $(LOCAL_PLT) deps/*/ebin ebin && \
			dialyzer --add_to_plt --plt $(LOCAL_PLT) --output_plt $(LOCAL_PLT) deps/*/ebin ebin ; test $$? -ne 1; \
		else \
			dialyzer --build_plt --output_plt $(LOCAL_PLT) deps/*/ebin ebin ; test $$? -ne 1; \
		fi \
	fi

dialyzer: ${PLT} ${LOCAL_PLT}
	@echo "==> $(shell basename $(shell pwd)) (dialyzer)"
	@if [ -f $(LOCAL_PLT) ]; then \
		PLTS="$(PLT) $(LOCAL_PLT)"; \
	else \
		PLTS=$(PLT); \
	fi; \
	if [ -f dialyzer.ignore-warnings ]; then \
		if [ $$(grep -cvE '[^[:space:]]' dialyzer.ignore-warnings) -ne 0 ]; then \
			echo "ERROR: dialyzer.ignore-warnings contains a blank/empty line, this will match all messages!"; \
			exit 1; \
		fi; \
		dialyzer $(DIALYZER_FLAGS) --plts $${PLTS} -c ebin > dialyzer_warnings ; \
		egrep -v "^[[:space:]]*(done|Checking|Proceeding|Compiling)" dialyzer_warnings | grep -F -f dialyzer.ignore-warnings -v > dialyzer_unhandled_warnings ; \
		cat dialyzer_unhandled_warnings ; \
		[ $$(cat dialyzer_unhandled_warnings | wc -l) -eq 0 ] ; \
	else \
		dialyzer $(DIALYZER_FLAGS) --plts $${PLTS} -c ebin; \
	fi

cleanplt:
	@echo
	@echo "Are you sure?  It takes several minutes to re-build."
	@echo Deleting $(PLT) and $(LOCAL_PLT) in 5 seconds.
	@echo
	sleep 5
	rm $(PLT)
	rm $(LOCAL_PLT)
