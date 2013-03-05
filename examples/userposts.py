import leveldbkit
import re

_crappy_email_regex = re.compile("^[_.0-9a-z-]+@([0-9a-z][0-9a-z-]+.)+[a-z]{2,4}$")

# Let's assume that this is a good email validator!
email_validator = lambda email: _crappy_email_regex.match(email) is not None


class Post(leveldbkit.Document):
  db = "./posts.db"

  content = leveldbkit.StringProperty()

class User(leveldbkit.Document):
  db = "./users.db" # This is the leveldb database that will be our users
  indexdb = "./users.indexes.db" # This is the leveldb database for the secondary indices

  name = leveldbkit.StringProperty() # a string property. Store any strings.
  email = leveldbkit.StringProperty(validators=email_validator, index=True)
  posts = leveldbkit.ListProperty(index=True)

if __name__ == "__main__":
  # connect to db. You don't need to do this as you declare db
  # and indexdb as LevelDB instances
  User.establish_connection()
  Post.establish_connection()

  # creating a new user
  user = User()
  user.name = "Shuhao"
  user.email = "shuhao@shuhaowu.com"
  user.save()

  # Finding me via my index: index is a generator:
  for u in User.index("email", "shuhao@shuhaowu.com"):
    shuhao = u # there is only 1.

  print shuhao.name # Shuhao
  print shuhao.email # shuhao@shuhaowu.com
  print shuhao.key # ....some unique identifier

  # Add a post

  post = Post(data={"content": "Hello World!"})
  shuhao.posts.append(post.key)
  shuhao.save()

  # Getting a user.
  shuhao = User.get(user.key) # user.key is my key
  print shuhao.posts # a list of keys..

  # Look up the author of a post!
  userkeys = User.index_keys_only("posts", post.key) # you could always use a simple index
  print userkeys[0]
  print shuhao.key # they're the same!

  # Try an invalid email

  user = User()
  user.email = "invalid"
  print user.is_valid() # False
  print user.invalids() # A list of properties that are invalid: should ["email"]

  # user.save() will raise ValidationError
