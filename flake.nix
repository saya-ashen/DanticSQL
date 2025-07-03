{
  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixpkgs-unstable";
    pyproject-nix = {
      url = "github:pyproject-nix/pyproject.nix";
      inputs.nixpkgs.follows = "nixpkgs";
    };

  };

  outputs =
    inputs:
    let
      system = "x86_64-linux";
    in
    {
      devShells.${system} =
        let
          pkgs = import inputs.nixpkgs {
            inherit system;
            overlays = [
              (self: super: rec {
                pythonldlibpath = inputs.nixpkgs.lib.makeLibraryPath (
                  with super;
                  [
                    zlib
                    zstd
                    stdenv.cc.cc
                    curl
                    openssl
                    attr
                    libssh
                    bzip2
                    libxml2
                    acl
                    libsodium
                    util-linux
                    xz
                    systemd
                  ]
                );
                # here we are overriding python program to add LD_LIBRARY_PATH to it's env
                python= super.stdenv.mkDerivation {
                  name = "python";
                  buildInputs = [ super.makeWrapper ];
                  src = super.python312;
                  installPhase = ''
                    mkdir -p $out/bin
                    cp -r $src/* $out/
                    wrapProgram $out/bin/python3 --set LD_LIBRARY_PATH ${pythonldlibpath}
                    wrapProgram $out/bin/python3.12 --set LD_LIBRARY_PATH ${pythonldlibpath}
                  '';
                };
              })
            ];

          };
        in
        {
          default = pkgs.mkShell {
            buildInputs = with pkgs; [
              uv
              python
              git
              just
              ripgrep
              pre-commit
            ];
            shellHook = ''
              VENV_DIR=".venv"
              if [ ! -d "$VENV_DIR" ]; then
                echo ">>> Virtual environment '$VENV_DIR' not found. Creating..."
                uv venv -p ${pkgs.python313}
                echo ">>> Virtual environment created."
              fi

              source "$VENV_DIR/bin/activate"

            '';
          };
        };
    };
}
