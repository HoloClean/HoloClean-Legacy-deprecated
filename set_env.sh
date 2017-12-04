export HOLOCLEANHOME="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
echo "Snorkel home directory: $HOLOCLEANHOME"
export PYTHONPATH="$PYTHONPATH:$HOLOCLEANHOME"
export PATH="$PATH:$HOLOCLEANHOME"
echo $PATH
echo "Environment variables set!"
