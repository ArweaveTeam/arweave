{ config, lib, pkgs, ... }:

let
  cfg = config.services.arweave;
  arweavePkg = pkgs.callPackage ./arweave.nix { inherit pkgs; crashDumpsDir = cfg.crashDumpsDir; erlangCookie = cfg.erlangCookie; };
  generatedConfigFile = "${import ./generate-config.nix { arweaveConfig = cfg; inherit pkgs; }}";
  arweave-service-start =
    let
      command = "${cfg.package}/bin/start-nix-foreground config_file ${cfg.configFile}";
      peers = "${builtins.concatStringsSep " " (builtins.concatMap (p: ["peer" p]) cfg.peer)}";
      vdf-peers = "${builtins.concatStringsSep " " (builtins.concatMap (p: ["vdf_client_peer" p]) cfg.vdfClientPeer)}";
      vdf-server-peers = "${builtins.concatStringsSep " " (builtins.concatMap (p: ["vdf_server_trusted_peer" p]) cfg.vdfServerTrustedPeer)}";
    in
    pkgs.writeScriptBin "arweave-start" ''
      #!${pkgs.bash}/bin/bash
      # Function to handle termination and cleanup
      cleanup() {
        echo "Terminating erl and killing epmd..."
        kill $(${pkgs.procps}/bin/pgrep epmd) || true
        exit 0
      }

      # Set up a trap to call the cleanup function when the script is terminated
      trap cleanup INT TERM
      ${command} ${peers} ${vdf-peers} ${vdf-server-peers} &
      ARWEAVE_ERL_PID=$! # capture PID of the background process
      i=0
      until [[ "$(${pkgs.procps}/bin/ps -C beam &> /dev/null)" -eq 0 || "$i" -ge "200" ]]
      do
        sleep 1
        i=$((i+1))
      done
      if [[ "$i" -ge "200" ]]; then
        echo "beam process failed to start"
        exit 0
      fi
      echo "beam process started..."
      wait $ARWEAVE_ERL_PID || true
      counter=0
      until [[ "$(ps -C beam &> /dev/null)" -ne 0 ]] || [[ $counter -ge 30 ]]
      do
        sleep 1
        let counter++
      done
      cleanup
    '';

in
{

  options.services.arweave = import ./options.nix {
    inherit lib;
    defaultArweaveConfigFile = generatedConfigFile;
    defaultArweavePackage = arweavePkg;
  };

  config = lib.mkIf cfg.enable {
    systemd.services.arweave-screen = {
      enable = false;
    };

    systemd.services.arweave = {
      after = [ "network.target" ];
      serviceConfig.Type = "simple";
      serviceConfig.ExecStart = "${arweave-service-start}/bin/arweave-start";
      serviceConfig.TimeoutStartSec = "60";
      serviceConfig.ExecStop = "${pkgs.bash}/bin/bash -c '${cfg.package}/bin/stop-nix || true; ${pkgs.procps}/bin/pkill beam || true; sleep 15'";
      serviceConfig.TimeoutStopSec = "120";
      serviceConfig.RestartKillSignal = "SIGINT";
    };
  };
}
