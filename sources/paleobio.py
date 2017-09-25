#
# Class for PaleoBio ingest
#

class paleobio:
    def __init__(self):
        self.source = "paleobio"

    # This is the main component of the ingester, and relies on a few different
    # helpers. But most of this code is specific to PaleoBio
    def runIngest(self, dry=False, test=False):
        # Should this be a dry or test run?
        dryRun = dry
        testRun = test
