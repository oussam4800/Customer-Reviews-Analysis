import time
import random
import pandas as pd
import re
import json
import unicodedata
import os
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
from selenium.common.exceptions import TimeoutException, NoSuchElementException, StaleElementReferenceException


save_folder = os.path.expanduser("~/airflow/reviews_DB_source")
os.makedirs(save_folder, exist_ok=True)

class GoogleMapsScraper:
    """Classe pour scraper Google Maps à la recherche d'agences CIH Bank au Maroc."""
    def __init__(self, implicit_wait=10, explicit_wait=10):
        """Initialisation du scraper avec configuration du navigateur."""
        options = webdriver.ChromeOptions()
        # Uncomment to run in headless mode
        # options.add_argument('--headless')
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
        options.add_argument('--disable-blink-features=AutomationControlled')
        options.add_argument('--window-size=1920,1080') 
        options.add_argument('--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.212 Safari/537.36')
        
        # Disable automation flags
        options.add_experimental_option("excludeSwitches", ["enable-automation"])
        options.add_experimental_option('useAutomationExtension', False)
        
        self.driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
        
        # Set implicit and explicit waits
        self.driver.implicitly_wait(implicit_wait)
        self.wait = WebDriverWait(self.driver, explicit_wait)

    @staticmethod
    def sanitize_filename(name):
        """Nettoie un nom de fichier pour éviter les caractères non valides."""
        name = unicodedata.normalize('NFKD', name).encode('ascii', 'ignore').decode('ascii')  # enlève accents
        name = re.sub(r'[^\w\s-]', '', name).strip().lower()
        return re.sub(r'[-\s]+', '_', name)
    
    @staticmethod
    def extract_city_from_address(address):
        """Extrait la ville à partir de l'adresse en prenant ce qui suit la dernière virgule."""
        match = re.search(r",\s*([^\d\n,]+)", address)
        if match:
            return match.group(1).strip()
        return "Ville inconnue"
    
    def search_places(self, query):
        """Recherche des lieux sur Google Maps."""
        print(f"Recherche de: {query}")
        self.driver.get("https://www.google.com/maps?hl=en")
        
        # Accepter les cookies si nécessaire
        try:
            cookie_button = self.wait.until(EC.element_to_be_clickable((By.XPATH, "//button[contains(text(), 'Tout accepter')]")))
            cookie_button.click()
            time.sleep(1)  # Réduit de 2s à 1s
        except:
            pass
        
        # Saisir la recherche
        search_box = self.wait.until(EC.presence_of_element_located((By.ID, "searchboxinput")))
        search_box.clear()
        search_box.send_keys(query)
        search_box.send_keys(Keys.ENTER)
        time.sleep(3)  # Réduit de 5s à 3s - juste assez pour charger les résultats
        
    def extract_agency_links(self, max_agencies=100):
        """Extrait les liens vers les agences depuis les résultats de recherche."""
        agency_links = []
        agencies_found = 0
        scroll_attempts = 0
        max_scroll_attempts = 30
    
        try:
        # Wait for the results container to be present
            results_container = self.wait.until(EC.presence_of_element_located(
                (By.CSS_SELECTOR, "div.m6QErb.DxyBCb.kA9KIf.dS8AEf")
            ))
        
            while agencies_found < max_agencies and scroll_attempts < max_scroll_attempts:
                # Find all visible results
                result_elements = self.driver.find_elements(By.CSS_SELECTOR, "a.hfpxzc")
            
                # Get links that aren't already in our list
                for element in result_elements:
                    try:
                        link = element.get_attribute('href')
                        if link and link not in agency_links:
                            agency_links.append(link)
                            agencies_found = len(agency_links)
                    except StaleElementReferenceException:
                        continue
            
                print(f"Agences trouvées: {agencies_found}")
            
                # Scroll to load more results
                try:
                    # Scroll to the last element to ensure new results load
                    if result_elements:
                        self.driver.execute_script("arguments[0].scrollIntoView(true);", result_elements[-1])
                    time.sleep(random.uniform(1.0, 1.5))
                
                    # Check if we've reached the end
                    new_elements = self.driver.find_elements(By.CSS_SELECTOR, "a.hfpxzc")
                    if len(new_elements) == len(result_elements):
                        scroll_attempts += 1
                    else:
                        scroll_attempts = 0
                except Exception as e:
                    print(f"Erreur lors du défilement: {e}")
                    scroll_attempts += 1
        
            print(f"Total des agences trouvées: {len(agency_links)}")
            return agency_links
        
        except Exception as e:
            print(f"Erreur lors de l'extraction des liens d'agences: {e}")
            return agency_links
    
    def get_place_details(self):
        """Récupère les détails du lieu (nom, adresse, note, nombre d'avis)."""
        try:
            # Attendre que les informations soient chargées
            self.wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "h1")))
            
            # Extraire le nom
            name = self.driver.find_element(By.CSS_SELECTOR, "h1").text
            
            # Extraire l'adresse
            try:
                address = self.driver.find_element(By.CSS_SELECTOR, "button[data-item-id='address']").text
            except:
                address = "Adresse non disponible"
            
            # Extraire la note et le nombre d'avis
            try:
                rating_text = self.driver.find_element(By.CSS_SELECTOR, "div.F7nice").text
                rating_parts = rating_text.split('\n')
                rating = rating_parts[0]
                num_reviews = re.search(r'(\d+)', rating_parts[1]).group(1) if len(rating_parts) > 1 else "0"
            except:
                rating = "N/A"
                num_reviews = "0"
            
            city = self.extract_city_from_address(address)

            return {
                "name": name,
                "address": address,
                "rating": rating,
                "num_reviews": num_reviews,
                "city": city
            }
        except Exception as e:
            print(f"Erreur lors de la récupération des détails: {e}")
            return {
                "name": "Erreur",
                "address": "Erreur",
                "rating": "Erreur",
                "num_reviews": "0",
                "city": "Ville inconnue"
            }
    
    def click_on_reviews(self):
        """Clique sur l'onglet des avis."""
        try:
            # Multiple strategies to find reviews button
            review_button_locators = [
                (By.XPATH, "//button[contains(@aria-label, 'Reviews')]"),
                (By.XPATH, "//button[contains(text(), 'Reviews')]"),
                (By.CSS_SELECTOR, "button[aria-label^='Reviews']"),
                (By.XPATH, "//button[contains(@aria-label, 'reviews')]"),
                (By.XPATH, "//button[contains(text(), 'reviews')]"),
                (By.CSS_SELECTOR, "button[jsaction*='pane.rating.moreReviews']"),  # ← nouveau locator
            ]
            
            for locator in review_button_locators:
                try:
                    reviews_button = self.wait.until(EC.element_to_be_clickable(locator))
                    reviews_button.click()
                    time.sleep(1.5)  # Réduit de 3s à 1.5s
                    print(f"Bouton d'avis trouvé avec locator: {locator}")
                    return True
                except:
                    continue
            
            print("Impossible de trouver le bouton des avis")
            return False
        except Exception as e:
            print(f"Impossible d'accéder aux avis: {e}")
            return False

    def scroll_reviews(self, target_reviews=100, max_attempts=30):
        """Fait défiler la page des avis pour en charger davantage. Réduit le nombre maximum de tentatives."""
        reviews_collected = 0
        attempts = 0
        
        while reviews_collected < target_reviews and attempts < max_attempts:
            try:
                # Multiple strategies to find scrollable element
                scroll_locators = [
                    (By.XPATH, "//div[contains(@role, 'feed')]"),
                    (By.CSS_SELECTOR, "div.m6QErb.DxyBCb"),
                    (By.CSS_SELECTOR, "div.m6QErb.scrollable-auto")
                ]
                
                scrollable_div = None
                for locator in scroll_locators:
                    try:
                        scrollable_div = self.wait.until(EC.presence_of_element_located(locator))
                        break
                    except:
                        continue
                
                if not scrollable_div:
                    print("Impossible de trouver l'élément de défilement")
                    break
                
                # Scroll using JavaScript
                last_height = self.driver.execute_script("return arguments[0].scrollHeight", scrollable_div)
                self.driver.execute_script("arguments[0].scrollTop = arguments[0].scrollHeight", scrollable_div)
                
                # Wait for new reviews to load - temps réduit
                time.sleep(random.uniform(0.8, 1.2))
                
                # Check number of reviews after scrolling
                current_reviews = len(self.driver.find_elements(By.CSS_SELECTOR, "div.jftiEf.fontBodyMedium"))
                
                if current_reviews > reviews_collected:
                    reviews_collected = current_reviews
                    print(f"Reviews collected so far: {reviews_collected}")
                    attempts = 0
                else:
                    attempts += 1
                
                # Check if we've reached the bottom
                new_height = self.driver.execute_script("return arguments[0].scrollHeight", scrollable_div)
                if new_height == last_height:
                    print("Reached bottom of reviews")
                    break
                
            except Exception as e:
                print(f"Scrolling error: {e}")
                attempts += 1
                time.sleep(random.uniform(1, 2))
        
        return reviews_collected

    def expand_reviews(self):
        """Développe tous les avis pour voir le texte complet."""
        try:
            # Trouver et cliquer sur tous les boutons "Plus"
            more_buttons = self.driver.find_elements(By.XPATH, "//button[contains(@jsaction, 'pane.review.expandReview')]")
            for button in more_buttons:
                try:
                    self.driver.execute_script("arguments[0].click();", button)
                    time.sleep(random.uniform(0.1, 0.2))  # Réduit de 0.2-0.5s à 0.1-0.2s
                except StaleElementReferenceException:
                    continue
        except Exception as e:
            print(f"Erreur lors de l'expansion des avis: {e}")

    def extract_reviews(self, target_reviews=30):
        """Extrait tous les avis visibles."""
        reviews = []
        try:
            # Get fresh review elements each time to avoid staleness
            review_elements = self.wait.until(
                EC.presence_of_all_elements_located((By.CSS_SELECTOR, "div.jftiEf.fontBodyMedium"))
            )
        
            for i in range(min(len(review_elements), target_reviews)):
                try:
                    # Get fresh reference to the element to avoid staleness
                    element = self.driver.find_elements(By.CSS_SELECTOR, "div.jftiEf.fontBodyMedium")[i]
                
                    # Nom de l'utilisateur
                    user_name = element.find_element(By.CSS_SELECTOR, "div.d4r55").text
                
                    # Note attribuée (nombre d'étoiles) - more robust handling
                    rating = None
                    try:
                        # Try multiple ways to find the rating
                        rating_elements = element.find_elements(By.CSS_SELECTOR, "span[aria-label*='étoiles'], span[aria-label*='star']")
                        if rating_elements:
                            rating_text = rating_elements[0].get_attribute('aria-label')
                            rating_match = re.search(r'(\d+(?:[.,]\d+)?)', rating_text)
                            if rating_match:
                                rating = float(rating_match.group(1).replace(',', '.'))
                    except Exception as rating_err:
                        print(f"Erreur de récupération de la note: {rating_err}")
                
                    # Date de l'avis
                    date = element.find_element(By.CSS_SELECTOR, "span.rsqaWe").text
                
                    # Texte de l'avis
                    try:
                        review_text = element.find_element(By.CSS_SELECTOR, "span.wiI7pd").text
                    except:
                        review_text = ""
                    if rating is None:
                        rating = 1.0
                    
                    reviews.append({
                        "user_name": user_name,
                        "rating": rating,
                        "date": date,
                        "text": review_text
                    })
                except StaleElementReferenceException:
                    print("Element became stale, skipping")
                    continue
                except Exception as e:
                    print(f"Erreur lors de l'extraction d'un avis: {e}")
                    continue
        
            print(f"Nombre total d'avis extraits : {len(reviews)}")
            return reviews
        except Exception as e:
            print(f"Erreur lors de l'extraction des avis: {e}")
            return []

    def scrape_agency(self, agency_link, target_reviews=100):
        """Scrape une agence spécifique: détails et avis."""
        try:
            print(f"Accès à l'agence: {agency_link}")
            self.driver.get(agency_link)
            time.sleep(2)  # Réduit de 5s à 2s
            
            place_details = self.get_place_details()
            
            all_reviews = []
            if self.click_on_reviews():
                # Scroll to load more reviews
                collected_reviews = self.scroll_reviews(target_reviews)
                
                # Expand reviews to see full text
                self.expand_reviews()
                
                # Extract reviews
                all_reviews = self.extract_reviews(target_reviews)
            
            return {
                "place_details": place_details,
                "reviews": all_reviews
            }
        except Exception as e:
            print(f"Erreur lors du scraping de l'agence: {e}")
            return {
                "place_details": {"name": "Erreur", "address": "Erreur", "rating": "Erreur", "num_reviews": "0"},
                "reviews": []
            }
    
    def scrape_all_cih_agencies(self, reviews_per_agency=5000, max_agencies=5000):
        """Recherche et scrape toutes les agences CIH Bank au Maroc."""
        # Recherche des agences CIH Bank au Maroc
        self.search_places("cih banque maroc")
        
        # Extraire les liens vers toutes les agences
        agency_links = self.extract_agency_links(max_agencies)
        
        # Tableau pour stocker tous les résultats
        all_results = []
        all_reviews = []
        
        # Scraper chaque agence une par une
        for i, link in enumerate(agency_links):
            print(f"\n======= Scraping de l'agence {i+1}/{len(agency_links)} =======")
            try:
                # Scraper l'agence
                result = self.scrape_agency(link, reviews_per_agency)
                
                # Ne sauvegarder que si on a des résultats valides
                if result["place_details"]["name"] != "Erreur":
                    all_results.append(result)
                    
                    # Ajouter les avis au dataframe
                    place_name = result["place_details"]["name"]
                    place_address = result["place_details"]["address"]
                    for review in result["reviews"]:
                        review["place_name"] = place_name
                        review["place_address"] = place_address
                        review["city"] = self.extract_city_from_address(place_address)
                        all_reviews.append(review)
                
                # Pause aléatoire entre les requêtes pour éviter les blocages
                # Cette pause est critique pour éviter d'être banni - gardée plus longue
                time.sleep(random.uniform(1.5, 3))
                
                # Nettoyer le nom du fichier
                safe_place_name = GoogleMapsScraper.sanitize_filename(result["place_details"]["name"])

                # Sauvegarde JSON avec nom nettoyé
                with open(os.path.join(save_folder, f"resultats_cih_agences_{i+1}_{safe_place_name}.json"), 'w', encoding='utf-8') as f:
                    json.dump(all_results, f, ensure_ascii=False, indent=4)

                # CSV avec les avis
                #if all_reviews:
                #    df = pd.DataFrame(all_reviews)
                #    df.to_csv(os.path.join(save_folder, f"avis_cih_banque_{i+1}_{safe_place_name}.csv"), index=False, encoding='utf-8')
                
            except Exception as e:
                print(f"Erreur lors du scraping de l'agence {i+1}: {e}")
        
        # Sauvegarde finale de tous les résultats
        with open(os.path.join(save_folder, "resultats_cih_banque_final.json"), 'w', encoding='utf-8') as f:
            json.dump(all_results, f, ensure_ascii=False, indent=4)
        
        # Création d'un DataFrame final pour analyse
        if all_reviews:
            final_df = pd.DataFrame(all_reviews)
            final_df.to_csv(os.path.join(save_folder, "avis_cih_banque_final.csv"), index=False, encoding='utf-8')
        
        return all_results
    
    def close(self):
        """Ferme le navigateur."""
        self.driver.quit()

def scraper():
    # Réduit les temps d'attente implicite et explicite
    scraper = GoogleMapsScraper(implicit_wait=10, explicit_wait=10)
    try:
        print("Démarrage du scraping des agences CIH Bank au Maroc")
        # Ajustez ces paramètres selon vos besoins
        results = scraper.scrape_all_cih_agencies(reviews_per_agency=20, max_agencies=3)
        print(f"Extraction terminée. {len(results)} agences analysées.")
    except Exception as e:
        print(f"Erreur lors du scraping: {e}")
    finally:
        scraper.close()
# Exemple d'utilisation
if __name__ == "__main__":
    scraper()