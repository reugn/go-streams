# Contributing

`go-streams` is an open-source project and welcomes contributions and suggestions.  
File an [issue](https://github.com/reugn/go-streams/issues) to report a bug or discuss a new feature.
Open a pull request to propose changes.

## Prerequisites

Go `1.21` is the minimum requirement for the core module;
see the [Download and Install](http://golang.org/doc/install) page for setup instructions.

## Contribution flow

* [Fork](https://docs.github.com/en/get-started/quickstart/fork-a-repo) the repository
* [Configure remote](https://help.github.com/en/github/collaborating-with-issues-and-pull-requests/configuring-a-remote-for-a-fork) for the fork
* `git checkout -b <your_branch>`
* `git add .`
* `git commit -m "commit message"`
* `git push --set-upstream origin <your_branch>`
* Verify all tests and CI checks pass
* [Create a pull request](https://help.github.com/en/github/collaborating-with-issues-and-pull-requests/creating-a-pull-request)

## Code style

* Refer to the [Code Review Comments](https://go.dev/wiki/CodeReviewComments) page for style guidelines
* Use [golangci-lint](https://golangci-lint.run) for static analysis of the source code
