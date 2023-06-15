# Contributing to `hostd`

Thank you for considering contributing to `hostd`! We welcome your contributions
and appreciate your effort in helping improve the project. To ensure a smooth
and efficient collaboration, we have outlined some best practices for
contributing. Please follow these guidelines to maintain clarity and
organization throughout the development process.

For users new to GitHub, we recommend reading the [GitHub Flow](https://guides.github.com/introduction/flow/)

## Fork the repository

To get started, fork the hostd repository to your GitHub account. This will
create a copy of the repository under your own account, allowing you to freely
make changes without affecting the original project.

## Create a descriptive branch

When working on a new feature or fixing a bug, create a new branch that clearly
describes the purpose of your changes. Use descriptive and meaningful names to
make it easier for others to understand the purpose of the branch. Only
implement one logical change per branch.

```sh
git checkout -b feature/descriptive-branch-name
```

## Keep History Clean with Rebasing

To keep the commit history clean and maintain a linear progression of changes,
use the git rebase command to incorporate the latest changes from the main
branch into your branch before submitting a pull request. This will help avoid
unnecessary merge commits.

```sh
git pull --rebase origin master
```

## Write descriptive commit messages

When committing changes, write a descriptive commit message that clearly and
concisely describes the purpose of the change.

## Lint & Test

Before submitting a pull request, run the linter and tests to ensure that your
changes do not break any existing functionality. If you are adding new
functionality, include tests to verify that the new code works as expected.
`hostd` uses [golangci-lint](https://golangci-lint.run/usage/install/) to lint.

### Run tests

```sh
go test -race -failfast -tags=testing ./...
```

### Run linter

1. Install golangci-lint
2. Run `golangci-lint run` to lint the code

## Submit a pull request

Once you have completed your changes and are ready to submit them, create a pull
request (PR) from your branch to the main repository. Provide a clear and
concise description of the changes you have made, including any relevant details
that can help reviewers understand the purpose and impact of your contributions.
