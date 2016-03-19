# locate16-hack
This [visualisation](http://54.252.134.107/) shows how Australian cities have grown over time - from the national scale, right down to the suburb level.

Using G-NAF address creation dates as a proxy for new housing - it shows the percentage and volume increase in addresses from 2010 to 2015.

![locate16-hack-image.png](https://github.com/minus34/locate16-hack/blob/master/locate16-hack-image.png "New addresses around Melbourne")

Worth noting - the final result is indicative only as G-NAF address are considered new when they are either brand new or if there is one of a number of changes to an G-NAF address (e.g. 10 Smyth St is changed to 10 Smith St). To counter this issue small changes in each hex area have been excluded. This issue is most noticeable where a suburb changes it's name - all addresses in the suburb are then considered new (e.g. Avalon in Sydney).
