#!/usr/bin/env sh
######################################################################
# Script used to export information about local software installed,
# useful in case of debugging.
######################################################################
CHECK_SOFTWARE="cc cmake cpp clang curl erl g++ gcc git make rsync wget pkg-config"
CHECK_LIBS="libssl gmp sqlite3 ncurses"

# function helper to check software version
_software_version() {
  local name="${1}"
  local flag="--version"
  test "${name}" = "erl" && flag="-version"

  if which "${name}" 2>&1 >/dev/null
  then
    local path=$(which "${name}")
    local version=$(${name} ${flag} 2>&1 | head -n1)
    echo "${name}:"
    echo "  path: ${path}"
    echo "  version: ${version}"
  else
    echo "${name}: not found"
  fi
}

# wrapper around erl command to easily evaluate erlang
# code from the shell
_erl() {
  local eval="${1}"
  local erl="erl
    -mode embedded -noshell -noinput
    -eval '${eval}.'
    -eval 'init:stop().'
  "
  eval ${erl}
}

# print erlang/beam information
_erlang_version() {
  if which erl 2>&1 >/dev/null
  then
    echo "erlang/beam:"
    _erl '
      io:format("\ \ root_dir: ~s~n", [code:root_dir()]),
      io:format("\ \ lib_dir: ~s~n", [code:lib_dir()]),
      io:format("\ \ modules:~n"),
      [
        io:format("\ \ \ \ ~s: ~s~n",[X,Y])
      ||
        {X,Y,_} <- code:all_available()
      ]
    '
  fi
}

# function helper to check library version
_lib_version() {
  local name="${1}"
  if pkg-config --exists "${name}"
  then
    echo "${name}:"
    echo "  version: $(pkg-config --modversion ${name})"
    echo "  flags: $(pkg-config --libs --cflags --define-prefix ${name})"
  else
    echo "${name}: not found"
  fi
}

######################################################################
# main script
######################################################################
# check software
for s in ${CHECK_SOFTWARE}
do
  _software_version ${s}
done

# check libraries
if $(which pkg-config 2>&1 >/dev/null)
then
  for l in ${CHECK_LIBS}
  do
    _lib_version ${l}
  done
fi

# check specific erlang vm and modules
_erlang_version
