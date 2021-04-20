.PHONY=stress check

# check runs one iteration of the load tests, good to ensure they still work after you change something.
check:
	k6 run pfs-get.js
	k6 run pfs-put.js

# stress runs 100 concurrent requests for a total of 1000 iterations, good if you are profiling pachd.
stress:
	k6 run pfs-get.js --vus 100 --iterations 1000
	k6 run pfs-put.js --vus 100 --iterations 1000
