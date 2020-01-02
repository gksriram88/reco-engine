from defines import article_reco, user_reco, user_data, article_data
import requests
from operator import itemgetter


class CassRecommendationService:
    def __init__(self, db, logger):
        self.db = db
        self.logger = logger

    def get_recommended_articles(self,article_id,user_guid, cosine_weight=1, euclidean_weight=0.5, overlap_weight=0.5):
        """
            The main function to be called externally
        
            Params: article_id and user_id are mandatory, the rest have default weight values 
        """
        self.logger.info("fetching recommended articles from reco engine")
        reco_articles = self.recommended_articles(article_id,user_guid, cosine_weight, euclidean_weight, overlap_weight)
        return reco_articles

    def recommended_articles(self, article_id, user_id, cosine_weight, euclidean_weight, overlap_weight):
        item_similarity_results, user_similarity_results = {}, {}
        negated_set = set()

        if article_id:
            item_similarity_results = self.item_similarity(article_id, cosine_weight, euclidean_weight)
            negated_set = set(item_similarity_results.keys())   # If there is no user record, we can atleast negate trending, news
        if user_id:
            user_similarity_results = self.user_similarity(user_id, overlap_weight)
            negated_set = self.negate_read_articles(user_id, item_similarity_results.keys(), user_similarity_results.keys())

        negated_trending = self.negate_trending(negated_set)
        negated_final = self.negate_news(negated_trending)
        final_recommendations = self.calculate_reco_scores(negated_final, item_similarity_results, user_similarity_results)
        return final_recommendations

    def item_similarity(self, article_id, cosine_weight, euclidean_weight):
        """
        Retrieves item similarity results and applies weights to the respective similarity
        Returns a dictionary in the form { 'article_id1': 'score1', 'article_id2': 'score2" }
        """
        self.logger.info('Starting item similarity for article id: ' + article_id)
        recommended_articles = {}
        result_set = self.db.execute("SELECT * FROM %s WHERE id='%s'" % (article_reco, article_id))
        for row in result_set:
            if row['cosine_ids'] and row['cosine_score']:
                for article, score in zip(row['cosine_ids'], row['cosine_score']):
                    recommended_articles[article] = score * cosine_weight

                # Add the knn scores if the articles are in common with cosine model
            if row['knn_ids'] and row['knn_score']:
                for article, score in zip(row['knn_ids'], row['knn_score']):
                    if article in recommended_articles.keys():
                        recommended_articles[article] += score * euclidean_weight
                    else:
                        recommended_articles[article] = score * euclidean_weight
        return recommended_articles

    def user_similarity(self, user_id, overlap_weight):
        """
        Retrieves user similarity and applies overlap_weight to it
        Returns a dictionary in the form { 'article_id1': 'score1', 'article_id2': 'score2" }
        """
        self.logger.info('Starting user similarity for user: ' + user_id)
        recommended_articles = {}

        result_set = self.db.execute("SELECT * FROM %s WHERE userid='%s'" % (user_reco, user_id))
        for row in result_set:
            for neighbour in row['neighbours']:
                neighbour_data = self.db.execute("SELECT * FROM %s WHERE userid='%s'" % (user_data, neighbour))
                for n_row in neighbour_data:
                    recommended_articles[n_row['articles'][-1]] = overlap_weight
        return recommended_articles

    def negate_read_articles(self, user_id, item_list, user_list):
        """
        Negates read history of user using set theoretic operations
        """
        self.logger.info("negating read articles for user=" + user_id)
        reco_set = set(item_list) | set(user_list)  # Union

        user_row = self.db.execute("SELECT * FROM %s WHERE userid='%s'" % (user_data, user_id))
        for row in user_row:
            user_article_set = set(row['articles'])

        return (reco_set - user_article_set)    # Non-symmetric difference

    def negate_trending(self, source_set):
        """
        Uses difference set theoretic operator to negate trending articles
        """
        self.logger.info("negating trending articles")
        trending = requests.get("https://www.scoopwhoop.com/api/v2/trendrealtime/").json()
        trending_set = set()

        for row in trending['data']:
            trending_set.add(row['_id'])  # Adding to a set

        return (source_set - trending_set)

    def negate_news(self, source_set):
        """
        Negates news category articles by adding to the destinations set only if it is not a category value
        """
        self.logger.info("negating news articles")
        dest_list = list()
        for article in source_set:
            article_details = self.db.execute("SELECT * FROM %s WHERE art_id='%s'" % (article_data, article))
            for row in article_details:
                if 'news' not in row['category']:
                    dest_list.append(row)
        return dest_list

    def calculate_reco_scores(self, negated_list, item_similarity_results, user_similarity_results):
        """
        Combines both the result lists and adds the scores if they have common elements.
        Anything that occurs in user similarity gets that as its type, else item similarity.
        
        Returns a reverse sorted list of dicts: [{'id': 'article_id_1', 'type': <user_smlr|item_smlr>, 'score': <score>}, ... ]
        """
        self.logger.info("calculating final recommendations")
        recommendations = []
        for article in negated_list:
            if article['art_id'] in user_similarity_results.keys():
                entry = {'id': article['art_id'],
                         'type': 'user_smlr', 
                         'score': user_similarity_results[article['art_id']],
                         'title': article['title'],
                         'slug': article['slug'],
                         'feature_img': article['feature_img'],
                         'category': article['category'],
                         'pub_date': article['pub_date']
                         }
                if article['art_id'] in item_similarity_results.keys():
                    entry['score'] += item_similarity_results[article]

            else:   # The only way an article gets here is by being in item similarity
                entry = {'id': article['art_id'],
                         'type': 'item_smlr',
                         'score': item_similarity_results[article['art_id']],
                         'title': article['title'],
                         'slug': article['slug'],
                         'feature_img': article['feature_img'],
                         'category': article['category'],
                         'pub_date': article['pub_date']
                         }
            recommendations.append(entry)

        return sorted(recommendations, key=itemgetter('score'), reverse=True)
