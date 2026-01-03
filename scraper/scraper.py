# imports
from bs4 import BeautifulSoup
import requests  # acts as a web browser for fetching pages, allows downloading HTML content
import json
import time  # allows sleeping
import os  # check computer files


# macros
BASE = "https://community.smartthings.com"  # main web address
HEADERS = {"user-agent": "SmartThingsResearchBot/1.0"}  # identify bot, shows its a script (helps avoid blocks)


# functions

# strip html
def htmlToText(htmlContent):
    soup = BeautifulSoup(htmlContent, 'html.parser')
    return soup.get_text(" ", strip=True)

# get all categories on the forum
def getCategories():
    url = f"{BASE}/categories.json"
    data = requests.get(url, headers=HEADERS).json()

    if data.status_code != 200:
        return None

    return data["category_list"]["categories"]

# get all forum topics from a category
def getTopics(categorySlug, pageNum):
    url = f"{BASE}/c/{categorySlug}.json?page={pageNum}"
    data = requests.get(url, headers=HEADERS).json()

    if data.status_code != 200:
        return None
    
    return data.json()

# fetch a specific topic
def getTopic(topicID):
    url = f"{BASE}/t/{topicID}.json"
    return requests.get(url, headers=HEADERS).json()

# loads already scraped topic ids (avoid redundancy)
def loadScraped():
    if not os.path.exists("scrapedTopics.txt"):  # file doesn't exist
        return set()
    return set(open("scrapedTopics.txt").read().split())  # read file, split by whitespace, convert to set

# appends topic to scraped topics file
def markScraped(topicID):
    with open("scrapedTopics.txt", "a") as file:
        file.write(str(topicID) + "\n")  # append topic ID to file

# saves posts to jsonl
def savePost(filename, record):
    with open(filename, "a", encoding="utf-8") as file:
        file.write(json.dumps(record) + "\n")  # write JSON record w newline

# actual crawl function
def crawl():
    categories = getCategories()
    scraped = loadScraped()
    
    for category in categories:
        cat = category["slug"]
        outputFile = f"data/{cat}.jsonl"
        page = 0  # start on first page of category

        while True:  # loop till no more topics
            listing = getTopics(category, page)  # download page of topics
            if listing is None:
                continue

            topics = listing["topic_list"]["topics"]  # extract list of topics
            if not topics:
                break  # no more topics

            for topic in topics:
                topicID = topic["id"]
                if str(topicID) in scraped:
                    continue  # skip, already scraped

                # get topic data and store each post
                topicData = getTopic(topicID)

                for post in topicData["post_stream"]["posts"]:
                    record = {
                        "category": category,
                        "topicID": topicID,
                        "topicTitle": topicData["title"],
                        "postID": post["id"],
                        "author": post["username"],
                        "timestamp": post["created_at"],
                        "content": htmlToText(post["cooked"])
                    }
                    savePost(outputFile, record)

                markScraped(topicID)
                time.sleep(1)  # be polite
            
            page += 1


# scrape function
# input url to scrape
def scrapeForumJson():
    url = "https://community.smartthings.com/t/using-smart-things-routines-to-control-baseboard/307172.json"
    data = requests.get(url).json()  # make GET request and parse JSON
    posts = data["post_stream"]["posts"]

    with open("forum_posts.csv", mode="w", newline="", encoding="utf-8") as file:
        writer = csv.writer(file)
        writer.writerow(["author", "timestamp", "content"])  # write header

        for post in posts:
            author = post["username"]
            timestamp = post["created_at"]
            content = post["cooked"]  # HTML content

            writer.writerow([author, timestamp, content])  # write post data


#### main
# main function
def main():
    categories = getCategories()
    for cat in categories:
        print(cat["id"], cat["name"], cat["slug"])

# entry point
if __name__ == "__main__":
    main()