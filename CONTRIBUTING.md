# How to contribute

If you would like to contribute code to this project, fork the repository and send a pull request.

## Prerequisite

If you have not installed Go, install it according to the [installation instruction](http://golang.org/doc/install).
Since the `go mod` package management tool is used in this project, **Go 1.11 or higher** version is required.

## Fork

Before contributing, you need to fork [go-streams](https://github.com/reugn/go-streams) to your github repository.

## Contribution flow

* [Configure remote for a fork](https://help.github.com/en/github/collaborating-with-issues-and-pull-requests/configuring-a-remote-for-a-fork)
* git checkout -b <your_branch>
* git add .
* git commit -m "commit message"
* git push --set-upstream origin <your_branch>
* Verify all tests and CI checks pass
* [Create a pull request](https://help.github.com/en/github/collaborating-with-issues-and-pull-requests/creating-a-pull-request)

## Code style

The coding style suggested by the Golang community is used in `go-streams`. For details, refer to [style doc](https://github.com/golang/go/wiki/CodeReviewComments).
