VEP v85 needs perl v5.20 or else it doesn't work correctly in hail. 
Google recently updated the default perl version in dataproc nodes from 5.20 to 5.24. It's difficult to directly revert this back to 5.20, so 
this docker image is used by vep85-GRCh37-init.sh and vep85-GRCh38-init.sh to make perl 5.20 (along with perl libraries necessary for VEP) 
available on dataproc nodes.


Commands:

docker login
docker build -t weisburd/vep-perl .     # build the docker image
docker push

Test runs:

echo "print 'works'" | docker run -i -v $(pwd):/root weisburd/vep-perl

docker run -v $(pwd):/root weisburd/vep-perl some_test_script.pl
