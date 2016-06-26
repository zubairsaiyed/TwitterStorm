from vaderSentiment.vaderSentiment import sentiment as vaderSentiment
import storm
import re

class VaderBolt(storm.BasicBolt):
    def process(self, tup):
        twt = tup.values[0].encode("utf-8","replace")
	# remove links
	twt = re.sub(r'https?:\S+', "", twt)

	# remove @ symbol
	twt = re.sub(r'@', "", twt)

	# split hashtags by camel case
	pattern = re.compile(r'#\S+')
	for match in re.findall(pattern, twt):
	    split = match.replace("#","")
	    split = re.sub(r"([A-Z])", " \\1", split)
	    split = re.sub(r"([a-zA-Z])([0-9])", "\\1 \\2", split)
	    split = split[1:] + "."
	    twt = twt.replace(match,split)
        
	#emoji_pattern = re.compile("["
        #u"\U0001F600-\U0001F64F"  # emoticons
        #u"\U0001F300-\U0001F5FF"  # symbols & pictographs
        #u"\U0001F680-\U0001F6FF"  # transport & map symbols
        #u"\U0001F1E0-\U0001F1FF"  # flags (iOS)
        #                   "]+", flags=re.UNICODE)
	#emoji_pattern.sub(r'',twt)
	
	vader = vaderSentiment(twt)['compound']

	for queryId in tup.values[1]:
        	storm.emit([queryId, vader, twt])

VaderBolt().run()
