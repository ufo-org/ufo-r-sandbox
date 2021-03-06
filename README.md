# UFO R Sandbox

This is a package that allows creation of custom UFOs with R code in their
populate and writeback functions. It executes that code in a sandbox R process
(hence the name). The sandbox is implemented by ufo-r-remote.

### Notes

* Entry function that starts the entire "server" loop, e.g. `ufosandbox::serve`.
  This sets up and calls a Rust function that starts waiting for incoming
  communication.
* 

## Installation

Before you can install this package, you need to install a working Rust toolchain. We recommend using [rustup.](https://rustup.rs/)

On Windows, you'll also have to add the `i686-pc-windows-gnu` and `x86_64-pc-windows-gnu` targets:
```
rustup target add x86_64-pc-windows-gnu
rustup target add i686-pc-windows-gnu
```

Once Rust is working, you can install this package via:
```r
remotes::install_github("ufo-org/ufo-r-sandbox")
```

<!-- After installation, the following should work:
```r
library(ufosandbox)

hello_world()
#> [1] "Hello world!"
``` -->

## Development

### Install rextendr

You will need [rextendr](https://github.com/extendr/rextendr) package to generate wrappers.
Please install it before proceeding to the next step.

``` r
remotes::install_github("extendr/rextendr")
```

### Generate wrappers

When you make either of the following changes to the Rust source code, you'll need to regenerate the wrappers.

* add a new function
* modify the signature of an existing function
* modify the documentation written on Rust code (on the lines starting with `///`)

This can be done by:

``` r
rextendr::document()
```

Which will compile the Rust code as well as updating documentation.

## Creating your own project

For a fully worked out demonstration of how to create a Rust + R library see [here](https://extendr.github.io/rextendr/articles/package.html).
