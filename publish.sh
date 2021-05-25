source .env/bin/activate
# pip install wheel
# pip install twine
python setup.py sdist bdist_wheel
twine upload dist/*
