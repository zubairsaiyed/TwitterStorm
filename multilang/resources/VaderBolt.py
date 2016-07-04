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

        # run VADER over normalized text
		vader = vaderSentiment(twt)['compound']

        # emit results for every matching queryId
		for queryId in tup.values[1]:
				storm.emit([queryId, vader, tup.values[0]])

VaderBolt().run()
