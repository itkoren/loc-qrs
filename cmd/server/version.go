package main

// version is set at build time via -ldflags="-X main.version=vX.Y.Z".
// It defaults to "dev" for local builds.
var version = "dev"
