clear
rm coverage.svg
pipenv run coverage run -m unittest discover -s test/ -vv
pipenv run coverage-badge -o coverage > build.sh
rm build.sh
