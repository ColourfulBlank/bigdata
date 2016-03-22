# Assignment 4

## w257wang
###Question 1: 
For each individual classifiers trained on group_x, group_y, and britney, what are the 1-ROCA% scores? You should be able to replicate our results on group_x, group_y, but there may be some non-determinism for britney, which is why we want you to report the figures.

+ group_x: 1-ROCA%: 17.25
+ group_y: 1-ROCA%: 12.82
+ britney: 1-ROCA%: 14.55

###Question 2: 
What is the 1-ROCA% score of the score averaging technique in the 3-classifier ensemble?

1-ROCA%: 11.99

###Question 3: 
What is the 1-ROCA% score of the voting technique in the 3-classifier ensemble?

1-ROCA%: 15.48

###Question 4: 
What is the 1-ROCA% score of a single classifier trained on all available training data concatenated together?

1-ROCA%: 14.85//19.94

###Question 5: 
Run the shuffle trainer 10 times on the britney dataset, predict and evaluate the classifier on the test data each time. Report the 1-ROCA% score in each of the ten trials and compute the overall average.

1. 1-ROCA%: 17.68
1. 1-ROCA%: 15.82
1. 1-ROCA%: 16.90
1. 1-ROCA%: 19.64
1. 1-ROCA%: 16.89
1. 1-ROCA%: 19.11
1. 1-ROCA%: 20.48
1. 1-ROCA%: 14.41
1. 1-ROCA%: 15.69
1. 1-ROCA%: 18.23
average: 17.485
