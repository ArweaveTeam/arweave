{ config, lib, pkgs, ... }:

let
  inherit (lib) literalExpression filterAttrs isAttrs isList mkEnableOption mkIf mkOption types;
  cfg = config.services.arweave;
  arweavePkg = pkgs.callPackage ./arweave.nix { inherit pkgs; };
  filterTopLevelNulls = set:
    let
      isNotNull = value: value != null;
    in
    filterAttrs (name: value: isNotNull value) set;
  generatedConfigFile = "${import ./generate-config.nix { arweaveConfig = cfg; inherit pkgs; }}";
in
{
  options.services.arweave = import ./options.nix {
    inherit lib;
    defaultArweaveConfigFile = generatedConfigFile;
    defaultArweavePackage = arweavePkg;
  };
  config = mkIf cfg.enable (
    let
      screen-watchdog = pkgs.writeScriptBin "arweave-watch-screen" ''
        #!${pkgs.bash}/bin/bash
        while true
        do
          if ! ${pkgs.screen}/bin/screen -ls | grep -E -q "[0-9]\.arweave"; then
            echo "arweace screen socket not detected, starting..."
            ${pkgs.screen}/bin/screen -dmS arweave -L -Logfile /var/lib/arweave/logs/arweave@screen-session.txt &
          fi
          sleep 1
        done
      '';

      arweave-service-pre-start = pkgs.writeScriptBin "arweave-pre-start" ''
        #!${pkgs.bash}/bin/bash
        # wait for screen socket for S-arweave to appear
        until [ "$(${pkgs.screen}/bin/screen -ls | grep 'S-arweave')" ]
        do
          sleep 1
        done
        exit 0
      '';

      arweave-service-start =
        let
          command = "${cfg.package}/bin/start-nix config_file ${cfg.configFile}";
          peers = "${builtins.concatStringsSep " " (builtins.concatMap (p: ["peer" p]) cfg.peer)}";
          vdf-peers = "${builtins.concatStringsSep " " (builtins.concatMap (p: ["vdf_client_peer" p]) cfg.vdfClientPeer)}";
          vdf-server-peers = "${builtins.concatStringsSep " " (builtins.concatMap (p: ["vdf_server_trusted_peer" p]) cfg.vdfServerTrustedPeer)}";
        in
        pkgs.writeScriptBin "arweave-start" ''
          #!${pkgs.bash}/bin/bash
          ${pkgs.screen}/bin/screen -S arweave -p 0 -X stuff "^C^M" || true
          ${pkgs.screen}/bin/screen -S arweave -p 0 -X stuff '${command} ${peers} ${vdf-peers} ${vdf-server-peers}^M'
          sleep 5
          until [[ "$(${pkgs.procps}/bin/ps -C epmd &> /dev/null)" -ne 0 ]]
          do
            sleep 1
          done
        '';

      arweave-service-stop = pkgs.writeScriptBin "arweave-stop" ''
        #!${pkgs.bash}/bin/bash
        ${cfg.package}/bin/stop-nix || true
      '';

      arweave-service-stop-post = pkgs.writeScriptBin "arweave-stop-post" ''
        #!${pkgs.bash}/bin/bash
        # wait for empd to die, otherwise kill it
        counter=0
        ${cfg.package}/bin/stop-nix || true
        until [[ "$(${pkgs.procps}/bin/ps -C epmd &> /dev/null)" -ne 0 ]] || [[ "$counter" -gt 15 ]]
        do
          sleep 1
          let counter++
        done
        [ "$(${pkgs.procps}/bin/ps -C epmd &> /dev/null)" ] || ${pkgs.procps}/bin/pkill epmd || true
        exit 0
      '';

    in
    {

      systemd.services.arweave-screen = {
        description = "A Service for starting Screen process";
        after = [ "network.target" ];
        environment = { };
        wantedBy = [ "multi-user.target" ];
        serviceConfig = {
          User = cfg.user;
          Group = cfg.group;
          Type = "simple";
          WorkingDirectory = "${cfg.package}";
          ExecStartPre = "${pkgs.bash}/bin/bash -c '${pkgs.procps}/bin/pkill screen || true; ${pkgs.screen}/bin/screen -wipe || true; sleep 1'";
          ExecStart = "${screen-watchdog}/bin/arweave-watch-screen";
        };
      };

      systemd.services.arweave = {
        description = "Arweave Node Service";
        after = [ "arweave-screen.service" ];
        environment = { };
        wantedBy = [ "multi-user.target" ];
        serviceConfig = {
          User = cfg.user;
          Group = cfg.group;
          WorkingDirectory = "${cfg.package}";
          Type = "simple";
          TimeoutStopSec = 30;
          ExecStartPre = "${arweave-service-pre-start}/bin/arweave-pre-start";
          ExecStart = "${arweave-service-start}/bin/arweave-start";
          ExecStop = "${arweave-service-stop}/bin/arweave-stop";
          ExecStopPost = "${arweave-service-stop-post}/bin/arweave-stop-post";
        };
      };
    }
  );
}
