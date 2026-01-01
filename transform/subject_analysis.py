import json
import pandas as pd
import spacy
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.decomposition import LatentDirichletAllocation
from collections import Counter
from transformers import pipeline
import re
from sklearn.feature_extraction.text import CountVectorizer, ENGLISH_STOP_WORDS
import os
import datetime
import numpy as np

analyseur = pipeline(
    task= "sentiment-analysis",
    model= "nlptown/bert-base-multilingual-uncased-sentiment", 
    device= 0                     # Utilise le GPU si disponible (mettre -1 pour CPU)
    )


# Load SpaCy model
nlp = spacy.load('en_core_web_sm')

def load_reviews(file_path):
    """
    Load review data from JSON file
    : return: 
        data frame;
            contain place adress, hashed user name, review and review publication date
    """
    
    df = pd.read_json(file_path, orient= "records")
    df_reviews = pd.json_normalize(
    df.to_dict(orient="records"),
    record_path="reviews",
    meta=[["place_details", "address"]],
    meta_prefix="place_"
    )
    
    # Sélection des colonnes voulues et renommage
    df_final = df_reviews[[
        "place_address",  # contenu de place_details.address
        "user_name",      # a encode
        "text",
        "date",
        "city"
    ]].rename(columns={"place_address": "place_address"})
    
    
    df_final.drop(columns= ["user_name"])
    return df_final

def extract_city_from_address(address):
        """Extrait la ville à partir de l'adresse en prenant ce qui suit la dernière virgule."""
        match = re.search(r",\s*([^\d\n,]+)", address)
        if match:
            return match.group(1).strip()
        return "Ville inconnue"

def preprocess_text(text):
    """Clean and preprocess review text"""
    if not isinstance(text, str):
        return ""
    # Remove excessive whitespace and newlines
    text = ' '.join(text.split())
    return text

def analyze_sentiment(text):
    """Analyze sentiment of text using NLTK's VADER"""
    
    # Skip empty or non-string texts
    if not text or not isinstance(text, str):
        return {
            'sentiment': 'neutral',
            'score' : 1
        }
    
    resultat = analyseur(text)
    if resultat[0]["label"] == "3 stars":
        sentiment = "NEUTRAL"
    elif resultat[0]["label"] < "3 stars":
        sentiment = "NEGATIVE"
    else:
        sentiment = "POSITIVE"
    return {
        "sentiment" : sentiment,
        "score": resultat[0]["score"]
    }
    

    
def extract_topics(text):
    
    """
    Extrait des topics (sujets) à partir d'une liste de textes français en utilisant LDA.

    Paramètres :
    - texts: list de str, les documents à analyser
    - num_topics: int, nombre de topics à extraire
    - num_words: int, nombre de mots à afficher par topic

    Retourne :
    - topics: dict où chaque clé est un 'Sujet i' et chaque valeur la liste des top mots
    """
    if not text or not isinstance(text, str):
        return None
    
    # Liste basique de stop-words français
    
    
    text = [text+ "no_topic"]
    num_words= max(([len(text) for text in text]))
    # 1. Vectorisation
    stop_words = list(ENGLISH_STOP_WORDS)
    vectorizer = CountVectorizer(stop_words= stop_words)
    dt_matrix = vectorizer.fit_transform(text)
    
    # 2. Modèle LDA
    lda = LatentDirichletAllocation(n_components= 1, random_state=0)
    lda.fit(dt_matrix)
    
    # 3. Extraction des mots-clés par topic
    terms = vectorizer.get_feature_names_out()
    topics = {}
    for idx, comp in enumerate(lda.components_):
        top_indices = comp.argsort()[:-num_words-1:-1]
        topics[f"Sujet {idx+1}"] = [terms[i] for i in top_indices]

    return topics

def analyze_reviews(df, inplace= False):
    """Analyze reviws reviews column

    Args:
        df (pd.DataFrame): data contains "text", "user_enc" ...
        inplace (bool, optional): modify df if True. Defaults to False.

    Returns:
        void : if inplace is True
        pd.DataFrame if inplace is False
    """
    
    sentiments_df = df["text"] \
    .apply(lambda t: pd.Series(analyze_sentiment(t))) \
    .rename(columns={"score": "sentiment_proba"})

    # On concatène côte à côte
    df_final = pd.concat([df, sentiments_df], axis=1)
    
    if inplace:
        df= df_final
        return
    return df_final

def topic_analysis(df, inplace= False):
    
    # On applique analyze_sentiment sur chaque texte, on récupère un Series pour chaque dict
    topics_df = df["text"]\
        .apply(lambda t: pd.Series(extract_topics(t)))
    if inplace:
        df["topics"]= topics_df
        return
    return topics_df

def date_tranformer(date):
    """Transforme date to number of years ago
    : params: 
        date (str): Combien de temp a passe sur la publication / dernière modification de l'avis
    : retuns:
        nombre d'années passe sur la publication / dernière modification
    """
    
    year_ago = None
    
    if "month" in date:
        year_ago = datetime.date.today().year
        
    if "year" in date:
        try:
            year_ago =datetime.date.today().year - int(date[:2])
        except:
            year_ago = datetime.date.today().year - 1
    return year_ago
        


def transformer():
    # File path to your JSON data - replace with actual df_final
    file_path = '~/airflow/reviews_DB_source/resultats_cih_banque_final.json'
    
    # Load review data
    reviews_data = load_reviews(file_path)
    
    if reviews_data.empty:
        print("No data to analyze. Please check the file.")
        return
    
    # Analyze reviews
    analyze_reviews(reviews_data, inplace= True)
    
    # Extraire les sujets
    topic_analysis(reviews_data, inplace= True)
   
    # Transformer la date
    reviews_data["date"] = reviews_data["date"].apply(date_tranformer).astype("Int64")
    
    # Verify DataFrame
    l = len(reviews_data)- len(reviews_data[reviews_data["text"] == ""])
    print(f"\nLoaded {l} reviews for analysis")
    print("\n===== SAMPLE ANALYSIS RESULTS =====")
    print(reviews_data.sample(5))
    
    
    
    # Save detailed results to CSV
    file_path = os.path.expanduser("~/airflow/reviews_DB_source/CIH_analysis_results.json")
    with open(file_path, "w", encoding="utf-8") as f:
        for _, row in reviews_data.iterrows():
            json.dump(row.to_dict(), f, indent=4, ensure_ascii=False)
            f.write("\n")  # Separate each record by newline
    reviews_data.to_csv('~/airflow/reviews_DB_source/CIH_analysis_results.csv', index= False)
    print(f"\n CIH_analysis_results.json")

if __name__ == "__main__":
    transformer()