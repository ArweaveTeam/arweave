{ pkgs }:


let
  gitignoreSrc = fetchFromGitHub {
    owner = "hercules-ci";
    repo = "gitignore.nix";
    rev = "211907489e9f198594c0eb0ca9256a1949c9d412";
    sha256 = "sha256-qHu3uZ/o9jBHiA3MEKHJ06k7w4heOhA+4HCSIvflRxo=";
  };

  inherit (import gitignoreSrc { inherit (pkgs) lib; }) gitignoreSource;
  inherit (pkgs) stdenv lib beamPackages fetchFromGitHub fetchFromGitLab fetchHex;

  randomx = fetchFromGitHub {
    owner = "arweaveteam";
    repo = "RandomX";
    rev = "d64fce8329f85bbafe43ffbfd03284242b13fb1c";
    sha256 = "sha256-+SrRGAasQcwo5gJm646Ci+31y6tJ0lgIAlzaeEez1CU=";
    fetchSubmodules = true;
  };

  buildRebar = beamPackages.buildRebar3.override { openssl = pkgs.openssl; };

  b64fast = buildRebar rec {
    name = "b64fast";
    version = "0.2.2";
    beamDeps = [ beamPackages.pc ];
    compilePort = true;

    src = fetchFromGitHub {
      owner = "arweaveteam";
      repo = name;
      rev = "a0ef55ec66ecf705848716c195bf45665f78818a";
      sha256 = "sha256-CSBsTRqkrQWwX7oxPZWERss5Pk0mE1ETe7s4fhZEUaA=";
      fetchSubmodules = true;
    };

    postBuild = ''
      env rebar3 pc compile
    '';
  };

  erlang-rocksdb = buildRebar rec {
    name = "erlang-rocksdb";
    version = "165d441f543c6be97e2f0df136628a736cabe85f";
    beamDeps = [ beamPackages.pc ];
    nativeBuildInputs = [ pkgs.cmake ];
    buildInputs = [ pkgs.getconf ];
    configurePhase = "true";
    src = fetchFromGitLab {
      owner = "hlolli";
      repo = name;
      rev = version;
      sha256 = "sha256-UjodnCNETaVwZd8dShJsPHIbePQkujo2wZNKZrRYnro=";
    };
    postInstall = ''
      mv $out/lib/erlang/lib/erlang-rocksdb-${version} $out/lib/erlang/lib/rocksdb-1.6.0
    '';
  };

  meck = buildRebar rec {
    name = "meck";
    version = "0.8.13";
    src = fetchHex {
      inherit version;
      pkg = name;
      sha256 = "sha256-008BPBVttRrVfMVWiRuXIOahwd9f4uFa+ZnITWzr6xo=";
    };
  };


  rebar3_hex = buildRebar {
    name = "rebar3_hex";
    version = "none";
    src = fetchFromGitHub {
      owner = "erlef";
      repo = "rebar3_hex";
      rev = "203466094b98fcbed9251efa1deeb69fefd8eb0a";
      sha256 = "gVmoRzinc4MgcdKtqgUBV5/TGeWulP5Cm1pTsSWa07c=";
      fetchSubmodules = true;
    };
  };

  geas_rebar3 = buildRebar {
    name = "geas_rebar3";
    version = "none";
    src = fetchFromGitHub {
      owner = "crownedgrouse";
      repo = "geas_rebar3";
      rev = "e3170a36af491b8c427652c0c57290011190b1fb";
      sha256 = "ooMalh8zZ94WlCBcvok5xb7a+7fui4/b+gnEEYpn7fE=";
    };
  };

  graphql = buildRebar {
    name = "graphql-erlang";
    version = "none";
    beamDeps = [ beamPackages.pc geas_rebar3 rebar3_hex ];

    patchPhase = ''
     substituteInPlace src/graphql.erl \
       --replace 'graphql/include/graphql.hrl' 'include/graphql.hrl'
     substituteInPlace src/graphql_ast.erl \
       --replace 'graphql/include/graphql.hrl' 'include/graphql.hrl'
     substituteInPlace src/graphql_err.erl \
       --replace 'graphql/include/graphql.hrl' 'include/graphql.hrl'
     substituteInPlace src/graphql_parser.yrl \
       --replace 'graphql/include/graphql.hrl' 'include/graphql.hrl'
     substituteInPlace src/graphql_introspection.erl \
       --replace 'graphql/include/graphql.hrl' 'include/graphql.hrl'
     substituteInPlace src/graphql_execute.erl \
       --replace 'graphql/include/graphql.hrl' 'include/graphql.hrl'
     substituteInPlace src/graphql_check.erl \
       --replace 'graphql/include/graphql.hrl' 'include/graphql.hrl'
    '';
    src = fetchFromGitHub {
      owner = "jlouis";
      repo = "graphql-erlang";
      rev = "4fd356294c2acea42a024366bc5a64661e4862d7";
      sha256 = "lJ6mEP5ab4GbFzlnbf9U9bAlZ+HGFZLbOZNvTUO1Dhw=";
    };
    postInstall = ''
      mv $out/lib/erlang/lib/graphql-erlang-none $out/lib/erlang/lib/graphql_erl-0.16.1
    '';
  };

  accept = buildRebar rec {
    name = "accept";
    version = "0.3.5";
    src = fetchHex {
      inherit version;
      pkg = name;
      sha256 = "sha256-EbGMIgvMLqtjtUcMA47xDrZ4O8sfzbEapBN976WsG7g=";
    };
  };

  double-conversion = fetchFromGitHub {
    owner = "google";
    repo = "double-conversion";
    rev = "32bc443c60c860eb6b4843533a614766d611172e";
    sha256 = "sha256-ysWwhvcVSWnF5HoJW0WB3MYpJ+dvqz3068G/uX9aBlU=";
  };

  jiffy = buildRebar rec {
    name = "jiffy";
    version = "1.0.8";
    setupHook = false;
    REBAR = "${pkgs.beamPackages.rebar3}/bin/rebar3";
    nativeBuildInputs = [ pkgs.pkg-config ];
    buildInputs = [ pkgs.llvmPackages.bintools-unwrapped ];
    configureFlags = [ "-fno-lto" ];
    hardeningDisable = [ "all" ];

    src = fetchFromGitHub {
      owner = "davisp";
      repo = name;
      rev = "37039ba32e950480715be74751a53339420a6fe1";
      sha256 = "sha256-t+AixeZ2HONEyyJ69CA7yJ5kWDthJM3c7J6jVriG7l0=";
    };

    patchPhase = ''
      sed -i -e 's|-compile.*||g' rebar.config
      rm -rf c_src/double-conversion
      cp -rf ${double-conversion}/double-conversion c_src/double-conversion
      chmod -R +rw c_src/double-conversion
    '';
  };

  prometheus = buildRebar rec {
    name = "prometheus";
    version = "4.6.0";
    src = fetchHex {
      inherit version;
      pkg = name;
      sha256 = "sha256-SQX9KZL4A47M16oM0i9AY37WGMC+0fdcBarOwVt1Rd4=";
    };
  };

  prometheus_httpd = buildRebar rec {
    name = "prometheus_httpd";
    version = "2.1.11";
    src = fetchHex {
      inherit version;
      pkg = name;
      sha256 = "sha256-C76DFFLP35WIU46y9XCybzDDSK2uXpWn2H81pZELz5I=";
    };
  };

  prometheus_cowboy = buildRebar rec {
    name = "prometheus_cowboy";
    version = "0.1.8";
    src = fetchHex {
      inherit version;
      pkg = name;
      sha256 = "sha256-uihr7KkwJhhBiJLTe81dxmmmzAAfTrbWr4X/gfP080w=";
    };
  };

  prometheus_process_collector = buildRebar rec {
    name = "prometheus_process_collector";
    version = "1.6.0";
    buildInputs = [ rebar3_archive_plugin rebar3_hex ];
    patchPhase = ''
      rm -rf .git
    '';

    src = fetchFromGitHub {
      owner = "deadtrickster";
      repo = name;
      rev = "78697537f01a858959a26a9c74db5aad2971b244";
      sha256 = "sha256-3Bb4d63JMdexzAI68Q+ASsj4FfNxQ9OUlG41fhFkMds=";
    };

    postInstall = ''
      mv $out/lib/erlang/lib/prometheus_process_collector-${version}/priv/source.so \
        $out/lib/erlang/lib/prometheus_process_collector-${version}/priv/prometheus_process_collector.so
    '';
  };

  rebar3_archive_plugin = buildRebar rec {
    name = "rebar3_archive_plugin";
    version = "0.0.2";
    src = fetchHex {
      inherit version;
      pkg = name;
      sha256 = "sha256-hMa0F1EdeazKg3WrLHXSD+zG0OK0C/puDz1i3OsyBYQ=";
    };
  };

  rebar3_elvis_plugin = buildRebar rec {
    name = "rebar3_elvis_plugin";
    version = "0b7dd1a3808dbe2e2e916ecf3afd1ff24e723021";
    src = fetchFromGitHub {
      owner = "deadtrickster";
      repo = name;
      rev = version;
      sha256 = "zM3WPLlbi05aYqMR5AhlNejBaPa6/nSIlq6CG7uNBoo=";
    };
  };

  cowlib = buildRebar rec {
    name = "cowlib";
    version = "e9448e5628c8c1d9083223ff973af8de31a566d1";
    src = fetchFromGitHub {
      owner = "ninenines";
      repo = "cowlib";
      rev = version;
      sha256 = "1j7b602hq9ndh0w3s7jcs923jclmiwfdmbfxaljcra5sl23ydwgf";
    };
  };

  cowboy = buildRebar rec {
    name = "cowboy";
    version = "2.9.0";
    buildInputs = [ cowlib rebar3_archive_plugin ranch ];
    beamDeps = [ cowlib rebar3_archive_plugin ranch ];
    plugins = [ beamPackages.pc ];
    src = fetchHex {
      inherit version;
      pkg = name;
      sha256 = "sha256-LHKfk0tOGqFJr/iC9XxjcsFTmaINVPZcjWe+9YMCG94=";
    };
  };

  gun = buildRebar rec {
    name = "gun";
    version = "1.3.3";
    beamDeps = [ beamPackages.pc geas_rebar3 rebar3_hex cowlib ];
    src = fetchHex {
      inherit version;
      pkg = name;
      sha256 = "sha256-MQbOFn+clyP4SeT7VOpKTYFOOZauJDocgoslbnSQQeA=";
    };
  };

  ranch = buildRebar rec {
    name = "ranch";
    version = "a692f44567034dacf5efcaa24a24183788594eb7";
    src = fetchFromGitHub {
      owner = "ninenines";
      repo = name;
      rev = version;
      sha256 = "03naawrq8qpv9al915didl4aicinj79f067isc21dbir0lhn1lgn";
    };
  };

  stopScript = pkgs.writeTextFile {
    name = "stop-nix";
    text = ''
      #! ${pkgs.stdenv.shell} -e

      PATH=
      ROOT_DIR=

      cd $ROOT_DIR
      export ERL_EPMD_ADDRESS=127.0.0.1

      erl -pa $(echo $ROOT_DIR/lib/*/ebin) \
        -noshell \
        -config config/sys.config \
        -name stopper@127.0.0.1 \
        -setcookie arweave \
        -s ar shutdown arweave@127.0.0.1 -s init stop
    '';
  };
  startScript = pkgs.writeTextFile {
    name = "start-nix";
    text = ''
      #! ${pkgs.stdenv.shell} -e

      PATH=
      ROOT_DIR=

      ERL_CRASH_DUMP=$(pwd)/erl_crash.dump
      cd $ROOT_DIR
      $ROOT_DIR/bin/check-nofile
      if [ $# -gt 0 ] && [ `uname -s` == "Darwin" ]; then
        RANDOMX_JIT="disable randomx_jit"
      else
        RANDOMX_JIT=
      fi

      export ERL_EPMD_ADDRESS=127.0.0.1

      erl +A100 +SDio100 +Bi -pa $(echo $ROOT_DIR/lib/*/ebin) \
       -config $ROOT_DIR/config/sys.config \
       -args_file $ROOT_DIR/config/vm.args.dev \
       -run ar main $RANDOMX_JIT "$@"

      # not the best practice: this assumes that no other epmd task are necessary
      # on the flipside, a zombied epmd causes arweave to fail to start next time
      sleep 20 && ${pkgs.procps}/bin/pkill epmd || true
    '';
  };


in beamPackages.rebar3Relx {

  pname = "arweave";
  version = "2.5.1-rc";
  src = gitignoreSource ../.;
  profile = "prod";
  releaseType = "release";
  plugins = [
    pkgs.beamPackages.pc
    rebar3_archive_plugin
    rebar3_elvis_plugin
  ];

  doStrip = false;

  nativeBuildInputs = with pkgs; [ clang-tools cmake pkg-config ];

  beamDeps = [
    beamPackages.pc
    geas_rebar3
    rebar3_hex
    b64fast
    erlang-rocksdb
    jiffy
    accept
    gun
    ranch
    cowlib
    meck
    cowboy
    prometheus
    prometheus_process_collector
    prometheus_cowboy
    prometheus_httpd
  ];

  buildInputs = with pkgs; [
    darwin.sigtool
    git
    gmp
    beamPackages.pc
    ncurses
    which
  ];

  postConfigure = ''
    rm -rf apps/arweave/lib/RandomX
    mkdir -p apps/arweave/lib/RandomX
    cp -rf ${randomx}/* apps/arweave/lib/RandomX
    cp -rf ${jiffy}/lib/erlang/lib/* apps/jiffy
    cp -rf ${graphql}/lib/erlang/lib/* apps/graphql
  '';

  postPatch = ''
    sed -i -e 's|-arch x86_64|-arch ${pkgs.stdenv.targetPlatform.linuxArch}|g' \
      apps/arweave/c_src/Makefile \
      apps/ar_sqlite3/c_src/Makefile

    sed -i -e 's|{b64fast,.*|{b64fast, "0.2.2"},|g' rebar.config
    sed -i -e 's|{graphql,.*|{graphql_erl, "0.16.1"},|g' rebar.config
    sed -i -e 's|"logs/"|"/var/lib/arweave/logs/"|g' apps/arweave/src/ar.erl
  '';

  installPhase = ''
    mkdir $out; cp -rf ./_build/prod/rel/arweave/* $out
    cp ${startScript.outPath} $out/bin/start-nix
    cp ${stopScript.outPath} $out/bin/stop-nix

    chmod +xw $out/bin/start-nix
    chmod +xw $out/bin/stop-nix

    sed -i -e "s|ROOT_DIR=|ROOT_DIR=$out|g" $out/bin/start-nix
    sed -i -e "s|PATH=|PATH=$PATH|g" $out/bin/start-nix

    sed -i -e "s|ROOT_DIR=|ROOT_DIR=$out|g" $out/bin/stop-nix
    sed -i -e "s|PATH=|PATH=$PATH|g" $out/bin/stop-nix

    cp -r ./config $out
  '';

}
