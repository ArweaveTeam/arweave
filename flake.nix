{
  description = "The Arweave server and App Developer Toolkit.";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixpkgs-unstable";
    utils.url = "github:numtide/flake-utils";
    flake-compat = {
      url = "github:edolstra/flake-compat";
      flake = false;
    };
  };

  outputs = { self, nixpkgs, utils, ... }: utils.lib.eachDefaultSystem (system:
    let
      pkgs = import nixpkgs { inherit system; };
      arweave = pkgs.callPackage ./nix/arweave.nix { inherit pkgs; };
    in
    {
      packages = utils.lib.flattenTree {
        inherit arweave;
      };

      nixosModules.arweave = {
        imports = [ ./nix/module.nix ];
        nixpkgs.overlays = [ (prev: final: { inherit arweave; }) ];
      };

      defaultPackage = self.packages."${system}".arweave;

      devShells = {
        # for arweave development, made to work with rebar3 builds (not nix)
        default = with pkgs; mkShellNoCC {
          name = "arweave-dev";
          buildInputs = [
            bashInteractive
            cmake
            elvis-erlang
            erlang
            erlang-ls
            gmp
            openssl
            pkg-config
            rebar3
            rsync
          ];

          PKG_CONFIG_PATH = "${openssl.dev}/lib/pkgconfig";

          shellHook = ''
            ${pkgs.fish}/bin/fish --interactive -C \
              '${pkgs.any-nix-shell}/bin/any-nix-shell fish --info-right | source'
            exit $?
          '';

        };
      };

    });

}
