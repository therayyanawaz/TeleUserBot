import sys
sys.path.append("/home/therayyanawaz/Documents/Github/TeleUserBot-main")
from ai_filter import _feed_segment_is_incomplete

print(_feed_segment_is_incomplete("Missile Wave Hits [Location"))
print(_feed_segment_is_incomplete("Missile Wave Hits [Location]"))
print(_feed_segment_is_incomplete("This is a normal sentence."))
