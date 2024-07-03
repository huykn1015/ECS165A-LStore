# ECS 165A (winter 2024) Team 13

## Local development

### Dev Containers
For local development, you can optionally use VS Code's Dev Containers. Ensure
that VS Code is installed and you have WSL2 on your system, then click on the
badge below to start a dev container from this repository. Alternatively, use
the VSCode command "Dev Containers: Clone Repository in Named Container Volume."

[![Open in Dev Containers](https://img.shields.io/static/v1?label=Dev%20Containers&message=Open&color=blue&logo=visualstudiocode)](https://vscode.dev/redirect?url=vscode://ms-vscode-remote.remote-containers/cloneInVolume?url=https://github.com/oakrc/ecs165a)

### Set up
This project uses Poetry to manage dependencies, but also have a requirements.txt
that is auto-updated by CI for the autograder.

If you are using Dev Containers, poetry should already be set up for you. Simply
run `poetry install --no-root` to install all dependencies. If you are not using
Dev Containers, run the following:

```shell
pip install pipx
pipx install poetry
poetry install --no-root
```

When you want to run the scripts locally, use `poetry shell` to launch the
virtual environment. To let VSCode see the installed packages in poetry's
virtual env (e.g., for intellisense), use `poetry config virtualenvs.in-project
true` and make sure to change the interpreter to poetry's.


### Contributing

Please open new Pull Requests if you want to merge your feature branch to `main`.
Later we might have unit tests to check that all the components we have implemented
are not failing.
