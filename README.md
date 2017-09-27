# cb-sdk-reproducer

How to run:
1. Have docker installed locally.
2. Clone this repository (don't build it).
3. Ensure no docker container named reproducer running.
4. Run src/test/resources/scripts/run-tests.sh

* When the test completes, the test container named reproducer will still be up.
* You can attach to the container using: docker exec -it reproducer bash
* Once you're done you can stop and remove the reproducer using: docker stop reproducer; docker rm reproducer 