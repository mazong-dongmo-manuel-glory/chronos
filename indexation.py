from sklearn.feature_extraction.text import TfidfVectorizer
from mongoengine import connect
from db import Page, Indexation

import numpy as np
import time

# Connect to MongoDB
connect('search')

# Create the TF-IDF transformer
vectorizer = TfidfVectorizer(use_idf=True, norm=None)

# Function to save TF-IDF scores to the Indexation collection
def save_tfidf_scores(page, score):
    score_dict = {word: tfidf_score for word, tfidf_score in zip(vectorizer.get_feature_names_out(), score)}
    Indexation(page_id=page.id, tfidf_scores=score_dict).save() 

# Number of documents to load at a time
batch_size = 1  # Process one page at a time

# Fit the vectorizer and transform all documents
contents = [page.content for page in Page.objects]
X = vectorizer.fit_transform(contents)

# Number of documents in the database
num_documents = len(Page.objects)

# Load and process the documents one by one
i = 0
while i < num_documents:
    # Get a page
    page = Page.objects.skip(i).first()

    # Get the content of the page
    content = page.content

    # Compute the TF-IDF for this page
    X_page = vectorizer.transform([content])

    # Save the TF-IDF scores to the Indexation collection
    save_tfidf_scores(page, X_page.toarray()[0])

    # Move to the next page
    i += 1

    # Wait for a short time before processing the next page
    time.sleep(0.5)  # Adjust the sleep time as needed

# Print a message indicating that the processing is complete
print("TF-IDF scores added to the Indexation collection.")
