{ pkgs ? import <nixpkgs> { } }:
  with pkgs;
  mkShell {
    packages = [cargo rustc redis rust-analyzer rustfmt];
  }

