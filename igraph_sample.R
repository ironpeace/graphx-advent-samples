install.packages("igraph")
library(igraph)

#edge <- c(1,2, 2,3, 3,1, 3,4, 4,5, 5,6, 6,4)
edge <- c(1,2, 2,3, 3,1, 4,5, 5,6, 6,4)

g <- graph(edge, directed=FALSE)

modularity(g, c(1,1,1,2,2,2))

modularity(g, c(1,2,3,4,5,6))

modularity(g, c(1,1,1,1,1,1))

modularity(g, c(1,1,1,1,2,2))

