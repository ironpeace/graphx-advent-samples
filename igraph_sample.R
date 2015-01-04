install.packages("igraph")
library(igraph)

a <- t(matrix(c(1,2, 2,3, 3,1, 3,4, 4,5, 5,6, 6,4), nrow=2))
a.igraph <- graph.edgelist(a)
modularity(a.igraph, membership=c(1,1,1,2,2,2))

modularity(a.igraph, membership=c(1,2,2,2,2,2))

modularity(a.igraph, membership=c(1,1,1,1,1,1))

modularity(a.igraph, membership=c(1,2,3,4,5,6))

modularity(a.igraph, membership=c(1,1,2,2,3,3))


a <- t(matrix(c(1,2, 2,3, 3,1, 3,4, 4,5, 5,6, 6,4, 7,8, 8,9, 7,9, 3,7, 4,7), nrow=2))
a.igraph <- graph.edgelist(a)
modularity(a.igraph, membership=c(1,1,1,2,2,2,3,3,3))
