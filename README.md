# batchgo
batchgo is a Golang library for batch processing data. It provides a simple way to process large amounts of data in batches.

![GitHub top language](https://img.shields.io/github/languages/top/WheeskyJack/batchgo) [![Go Report Card](https://goreportcard.com/badge/github.com/WheeskyJack/batchgo)](https://goreportcard.com/report/github.com/WheeskyJack/batchgo)

## Features
 1. Asynchronous batch processing
 2. concurrency-safe mechanism to add item to batch
 3. `Slicer` interface empowers users to merge or append data

 ## Go Docs

   Documentation at <a href="https://pkg.go.dev/github.com/WheeskyJack/batchgo">pkg.go.dev</a>

## Installation

    go get github.com/WheeskyJack/batchgo

## Usage
The user needs to implement the Slicer interface on the batching object for this package to work. It has `Append`, `Len` `Export` and `OnFailure` methods.

`Append` method is called to merge the item into existing batch.

`Len` method is called to get the length of current batch.

`Export` method is called once batch is full and ready for processing.

`OnFailure` method is called if Export method fails.

Please refer to the example directory for a working demonstration. It showcases how `Add()` can be called concurrently for batching.

## Contributing
Contributions are welcome! Please feel free to create an issue and submit a pull request.

## License

This project is licensed under the <a href="https://github.com/WheeskyJack/batchgo/blob/main/LICENSE">MIT License</a> - see the LICENSE file for details.