# Notes on the Readwise Data

## General

- Highlights are definitively not in any order. Use "location".
- You can edit highlights - in app, the original version is retained.
    - Based on an article at least, the API outputs the EDITED version with no access to
    the original text

## Podcasts

### Author
- "author" is the name of the podcast e.g. "The Rest is History"

### Entry Names
- each episode is it's own "book" therefore, podcast names aren't guaranteed to be consistent

### Formatting
The output format varies. Seems to be primarily:

- Old format: Heading and paragraph(s) of text with Speaker 1/Speaker 2
- New format: Title/No title, then bullets (two for short, more for long) and then named speakers

However, there are definitely other variations:

- Some entries use `\u2022` as bullets instead of `-`
- Some entries seem to state `'Key Takeaways'` before the summary paragraph?

#### Extracting transcript and speakers

In both formats transcripts always(??) begins with": `\n\nTranscript:\n<Speaker1>` and
from then on, a double newline indicates another speaker e.g. `\n\nSpeaker 2\n`.

The new format email announcement was on 11th October (at least one of my podcasts from
that date has highlights of both types).

#### AI Clips
- In SNIPD, new auto AI clips can't be discerned from user clips
    - Might be possible using a custom prompt?
    - Then automate adding tags

#### Episode AI Notes
Older episodes always/often had these which would have the heading `Episode AI notes`

## Articles

- "author" is mostly useful
- they could do with a tidy up
    - e.g what is Choc Choc shop (https://readwise.io/bookreview/43183293)

## YouTube Transcripts

They live in the articles category.

Can be pulled out via: `"source_url": "https://www.youtube.com/watch`


## Tweets / Tweet Threads

Tweet threads are saved in the tweets category but are handled differently.

- All tweet images (should) start "https://pbs.twimg"
- Tweets vs Tweet threads:
    - Tweet threads are handled differently within the tweet category
    - INDIVIDUAL TWEET:
        - `"title": "Tweets From <Display Name>"`
        - `"author": "@<Username> on Twitter"`
    - TWEET THREAD:
        - ``"title": "<First 24 chars of tweet>..."`
        - ``"author": "@<Username> on Twitter"``,
        ("@sama on Twitter" good example for both types)
- There seems to be no way to discern if a tweet includes a retweet or not???
    - May need to screenshot every tweet anyway? Might be nice regardless?


- Video links will render as HTML tags e.g.

```
"Jensen Huang got his best career advice from a gardener in Japan.
<video controls>
<source src=\"https://video.twimg.com/amplify_video/1856687129281155072/pl/DnPYfotYwx8_rn75.m3u8?tag=16\" type=\"application/x-mpegURL\">

<source src=\"https://video.twimg.com/amplify_video/1856687129281155072/vid/avc1/320x320/SMuHeA9MBI1JbzZB.mp4?tag=16\" type=\"video/mp4\">

<source src=\"https://video.twimg.com/amplify_video/1856687129281155072/vid/avc1/540x540/oSpAGIaXmqLPLcuG.mp4?tag=16\" type=\"video/mp4\">

<source src=\"https://video.twimg.com/amplify_video/1856687129281155072/vid/avc1/720x720/hS8r-NnrZMBoPKb1.mp4?tag=16\" type=\"video/mp4\"> Your browser does not support the video tag.</video>",
```
