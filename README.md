# An automated index tuner

An automated index tuner for the course [15799 at CMU](https://15799.courses.cs.cmu.edu/spring2022/).

The tuner first identifies relevant groups of indexes from a given workload log. Then it tries to evaluate potential indexes by randomly selecting one set of indexes. Indexes of all widths are considered.

The given queries are clustered based by extracting out their templates. This allows the tuner to quickly test out one representative query for each cluster. The query plan cost is scaled based on cluster frequency to ensure that the cost estimate is similar to that of running all queries.