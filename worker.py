# POST TO IG USING FB TOKEN + IG ACCOUNT ID
def post_ig(link):
    if not IG_USER_ID or not FB_TOKEN: return False
    try:
        # Step 1: Create media container
        url = f"https://graph.facebook.com/v20.0/{IG_USER_ID}/media"
        payload = {
            'image_url': 'https://yourdomain.com/deal.jpg',  # HOST IMAGE PUBLICLY
            'caption': f"Hot deal! {link}",
            'access_token': FB_TOKEN
        }
        r = requests.post(url, data=payload)
        if r.status_code != 200: return False
        creation_id = r.json()['id']

        # Step 2: Publish
        url = f"https://graph.facebook.com/v20.0/{IG_USER_ID}/media_publish"
        payload = {
            'creation_id': creation_id,
            'access_token': FB_TOKEN
        }
        r = requests.post(url, data=payload)
        return r.status_code == 200
    except:
        return False
