######################################################################
# Arweave Release GNU Makefile for MacOS
#
# This Makefile was created to build release on Darwin/MacOS system
# using homebrew package manage. Every build is created using a
# fresh version of the arweave git repository isolated from other
# build.
#
# To install dependencies and create all releases based on Erlang
# defined in ERLANG_VERSIONS variables:
#
#     make all
#
# To install only one release using one erlang version:
#
#     make build-release ERLANG_VERSION=24
#
######################################################################
ARWEAVE_GIT_TAG ?= master
ARWEAVE_REPOSITORY ?= https://github.com/ArweaveTeam/arweave.git
ERLANG_VERSIONS ?= 24 26
ERLANG_VERSION ?= 24
BUILDDIR ?= ./build
RELEASEDIR ?= ./release
SYSTEM_NAME = $(shell uname -o)
SYSTEM_ARCH = $(shell uname -m)
ARWEAVE_RELEASE_NAME ?= $(ARWEAVE_GIT_TAG)-$(SYSTEM_NAME)-$(SYSTEM_ARCH)
HOMEBREW_PATH = /opt/homebrew
HOMEBREW_COMMAND ?= brew

######################################################################
# default entry-point target
######################################################################
PHONY += help
help:
	@echo "Usage: make [help|install-deps|all|build-release|clean|clean-all]"
	@echo "  help: print help message"
	@echo "  install-deps: install dependencies with homebrew"
	@echo "  all: create all release"
	@echo "  build-release: create release using default erlang version"
	@echo "  clean: remove built artifacts"
	@echo "  clean-all: remove built artifacts and releases"
	@echo "Variables:"
	@echo "  ARWEAVE_GIT_TAG=$(ARWEAVE_GIT_TAG)"
	@echo "  ARWEAVE_RELEASE_NAME=$(ARWEAVE_RELEASE_NAME)"
	@echo "  ARWEAVE_REPOSITORY=$(ARWEAVE_REPOSITORY)"
	@echo "  BUILDDIR=$(BUILDDIR)"
	@echo "  ERLANG_VERSION=$(ERLANG_VERSION)"
	@echo "  ERLANG_VERSIONS=$(ERLANG_VERSIONS)"
	@echo "  RELEASEDIR=$(RELEASEDIR)"
ifneq ($(SYSTEM_NAME), Darwin)
	@echo "WARNING: this Makefile is not compatible with this system: $(SYSTEM_NAME)"
endif

######################################################################
# template to install/cleanup erlang using brew
######################################################################
define template_erlang
DEPS_$(1) += $(HOMEBREW_PATH)/opt/erlang@$(1)
HOMEBREW_ERLANG_DEPS += $(HOMEBREW_PATH)/opt/erlang@$(1)
$(HOMEBREW_PATH)/opt/erlang@$(1):
	$(HOMEBREW_COMMAND) install erlang@$(1)

HOMEBREW_CLEAN += clean-erlang-$(1)
PHONY += clean-erlang-$(1)
clean-erlang-$(1):
	-$(HOMEBREW_COMMAND) uninstall erlang@$(1)
endef

######################################################################
# template to install/release arweave
######################################################################
define template_builder
$$(BUILDDIR)/arweave-$(1): $$(BUILDDIR)
	git clone --recursive $$(ARWEAVE_REPOSITORY) $$(BUILDDIR)/arweave-$(1)

RELEASE_$(1) += $$(RELEASEDIR)/$$(ARWEAVE_RELEASE_NAME)-R$(1).tar.gz
RELEASES += $$(RELEASEDIR)/$$(ARWEAVE_RELEASE_NAME)-R$(1).tar.gz
ALL += $$(RELEASES)
$$(RELEASEDIR)/$$(ARWEAVE_RELEASE_NAME)-R$(1).tar.gz: $$(RELEASEDIR) $$(BUILDDIR)/arweave-$(1)
	git -C $$(BUILDDIR)/arweave-$(1) fetch --all
	git -C $$(BUILDDIR)/arweave-$(1) pull --force
	git -C $$(BUILDDIR)/arweave-$(1) checkout --force $$(ARWEAVE_GIT_TAG)
	git -C $$(BUILDDIR)/arweave-$(1) submodule update
	cd $$(BUILDDIR)/arweave-$(1) \
		&& export PATH="/opt/homebrew/opt/erlang@$(1)/bin:/opt/homebrew/bin:$${PATH}" \
		&& ./rebar3 as prod tar
	cp $$(BUILDDIR)/arweave-$(1)/_build/prod/rel/arweave/arweave-*.tar.gz $$@

CHECKSUM_$(1) += $$(RELEASEDIR)/$$(ARWEAVE_RELEASE_NAME)-R$(1).tar.gz.sha256
CHECKSUMS += $$(RELEASEDIR)/$$(ARWEAVE_RELEASE_NAME)-R$(1).tar.gz.sha256
ALL += $$(CHECKSUMS)
$$(RELEASEDIR)/$$(ARWEAVE_RELEASE_NAME)-R$(1).tar.gz.sha256: $$(RELEASEDIR)/$$(ARWEAVE_RELEASE_NAME)-R$(1).tar.gz
	sha256sum $$(RELEASEDIR)/$$(ARWEAVE_RELEASE_NAME)-R$(1).tar.gz \
		> $$@

ARWEAVE_CLEAN += clean-arweave-$(1)
PHONY += clean-arweave-$(1)
clean-arweave-$(1):
	-rm -rf $$(BUILDDIR)/arweave-$(1)

ARWEAVE_CHECKSUMS_CLEAN += clean-arweave-checksum-$(1)
PHONY += clean-arweave-checksum-$(1)
clean-arweave-checksum-$(1):
	-rm $$(RELEASEDIR)/$$(ARWEAVE_RELEASE_NAME)-R$(1).tar.gz.sha256
endef

######################################################################
# main directories
######################################################################
$(BUILDDIR):
	mkdir -p $@

$(RELEASEDIR):
	mkdir -p $@

######################################################################
# homebrew deps targets
######################################################################
$(foreach v, $(ERLANG_VERSIONS), $(eval $(call template_erlang,$(v))))

# gmp dep
HOMEBREW_DEPS += $(HOMEBREW_PATH)/Cellar/gmp
$(HOMEBREW_PATH)/Cellar/gmp:
	$(HOMEBREW_COMMAND) install gmp

HOMEBREW_CLEAN += clean-homebrew-gmp
PHONY += clean-homebrew-gmp
clean-homebrew-gmp:
	-$(HOMEBREW_COMMAND) uninstall gmp

# pkg-config dep
HOMEBREW_DEPS += $(HOMEBREW_PATH)/Cellar/pkgconf
$(HOMEBREW_PATH)/Cellar/pkgconf:
	$(HOMEBREW_COMMAND) install pkg-config

HOMEBREW_CLEAN += clean-homebrew-pkg-config
PHONY += clean-homebrew-pkg-config
clean-homebrew-pkg-config:
	-$(HOMEBREW_COMMAND) uninstall pkg-config

# cmake dep
HOMEBREW_DEPS += $(HOMEBREW_PATH)/Cellar/cmake
$(HOMEBREW_PATH)/Cellar/cmake:
	$(HOMEBREW_COMMAND) install cmake

HOMEBREW_CLEAN += clean-homebrew-cmake
PHONY += clean-homebrew-cmake
clean-homebrew-cmake:
	-$(HOMEBREW_COMMAND) uninstall cmake

######################################################################
# arweave targets
######################################################################
$(foreach v, $(ERLANG_VERSIONS), $(eval $(call template_builder,$(v))))

######################################################################
# main targets.
######################################################################
ifneq ($(SYSTEM_NAME), Darwin)
all:
	@echo "This Makefile was created for MacOS/Darwin system only"
	@exit 1

else
PHONY += all
all: install-deps $(ALL)

PHONY += build-release
build-release: $(HOMEBREW_DEPS) \
	$(DEPS_$(ERLANG_VERSION)) \
	$(RELEASE_$(ERLANG_VERSION)) \
	$(CHECKSUM_$(ERLANG_VERSION))

PHONY += build-checksum
build-checksum: $(CHECKSUMS)

PHONY += install-deps
install-deps: deps-update $(HOMEBREW_DEPS) $(HOMEBREW_ERLANG_DEPS)

PHONY += deps-update
deps-update:
	$(HOMEBREW_COMMAND) update

PHONY += clean-deps
clean-deps: $(HOMEBREW_CLEAN)

PHONY += clean
clean: clean-deps $(ARWEAVE_CLEAN)

PHONY += clean-all
clean-all: clean $(ARWEAVE_CHECKSUMS_CLEAN)
	-rm $(RELEASES)

.PHONY: $(PHONY)
endif

