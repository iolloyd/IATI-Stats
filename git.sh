#!/bin/bash

# Clear gitout
##cd gitout || exit $?
##git rm -r *
##rm -rf *
##cd .. || exit $?

python2 ckan.py

# Clear other output directories
rm -r aggregated*
rm -r inverted*
rm -r out*

cd data || exit $?
# Checkout automatic, and make sure it is clean and up to date
git reset --hard
git clean -df
git checkout automatic
git pull --ff-only

# Create gitdate file
echo '{' > gitdate.json
git log --format="format:%H|%ai" | awk -F '|' '{ print "\""$1"\": \""$2"\"," } ' >> gitdate.json
# Ensure the last line doesn't have a trailing comma
sed -i '$d' gitdate.json
git log --format="format:%H|%ai" | tail -n1 | awk -F '|' '{ print "\""$1"\": \""$2"\"" } ' >> gitdate.json
echo '}' >> gitdate.json
# Perform this dance because piping to ../ behaves differently with symlinks
cd .. || exit $?
mv data/gitdate.json .
cd data || exit $?

current_hash=`git rev-parse HEAD`

for commit in `git log --format=format:%H`; do
    if [ ! -e ../gitout/$commit ]; then
        git checkout $commit
        git clean -df
        echo $commit;
        cd .. || exit $?
        mkdir aggregated
        #python2 loop.py $@
        nice -n 10 python2 loop.py --symlinks --multi 4 $@
        mkdir gitout/$commit
        mv out gitout/$commit
        cd data || exit $?
    fi
done

git checkout automatic

for commit in `git log --format=format:%H`; do
    if [ ! -e ../git/$commit ]; then
        echo $commit
        cd .. || exit $?
        ln -s gitout/$commit/out out
        python2 aggregate.py
        python2 invert.py
        mkdir git/$commit
        mv aggregated* inverted* git/$commit || exit $?
        rm out
        cd data || exit $?
    fi
done

cd ..
python2 gitaggregate.py > git/gitaggregate.json
python2 gitaggregate.py dated > git/gitaggregate-dated.json
mv gitdate.json git
mv ckan.json git

cd git || exit $?
rm current
ln -s $current_hash current
tar -hczf current.tar.gz current

