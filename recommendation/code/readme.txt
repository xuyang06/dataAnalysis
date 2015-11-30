I have three directory in my final codes:
1. directory datasetconvert:
This directory contains the codes about how to convert initial yelp dataset to the format that we wanted.
2. directory ItemRecommender:
This directory contains the codes about how to do matrix factorization using hadoop
2.1 subdirecory com.data.reader:
This subdirecory contains the codes that how to use hadoop to realize matrix factorization.
2.2 subdirecory org.thunlp:
This subdirecory contains the codes that how to use hadoop to realize topic modeling for a given text. My intial plan is that I will combine rating matrix factorization with review topic modeling techniques to develop a new recommendation model. But due to the time constraint, I haven't used any review information yet. Those codes are only for future usage.
3. directory recommender:
This directory contains the codes that realize recommendation model (popularity model, item similarity model, matrix factorization model) without hadoop.