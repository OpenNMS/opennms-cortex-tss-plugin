<!-- Describe what this PR changes and why. -->

### All Contributors

* [ ] Have you read our [Contribution Guidelines](https://github.com/OpenNMS-Plugins/opennms-prometheus-remotewrite-plugin/blob/main/CONTRIBUTING.md)?
* [ ] Are your commits signed off (`git commit -s`), certifying the [Developer Certificate of Origin](https://developercertificate.org/)?

### Contribution Checklist

* Plugin software management is driven in the PNNMS project. Please [make an issue in the OpenNMS issue tracker](https://opennms.atlassian.net/browse/PNNMS) if there isn't one already.<br />Once there is an issue, please:
  1. update the title of this PR to be in the format: `${JIRA-ISSUE-NUMBER}: subject of pull request`
  2. set the fix version on the issue to the next unreleased patch release
  3. update the Jira link at the bottom of this comment to refer to the real issue number
  4. prefix your commit messages with the issue number, if possible
  5. once you've created this PR, please link to it in a comment in the Jira issue
  Don't worry if this sounds like a lot, we can help you get things set up properly.
* If this is a new or updated feature, is there documentation for the new behavior?
* If this is new code, are there unit and/or integration tests?
* Does `mvn clean install` pass locally?
* **If an AI coding tool helped write this, do those commits carry an `Assisted-by` trailer?**

### What's Next?

A PR should be assigned at least 2 reviewers.  If you know that someone would be a good person to review your code, feel free to add them.

If you need help making additions or changes to the documentation related to your changes, please let us know.

In any case, if anything is unclear or you want help getting your PR ready for merge, please don't hesitate to say something in the comments here,
or in [the #opennms-development chat channel](https://chat.opennms.com/opennms/channels/opennms-development).

Once reviewer(s) accept the PR and the branch passes continuous integration, the PR is eligible for merge.

At that time, if you have commit access (are an OpenNMS Group employee or a member of the OGP) you are welcome to merge the PR when you're ready.
Otherwise, a reviewer can merge it for you.

Thanks for taking time to contribute!

### External References

* Jira (Issue Tracker): https://opennms.atlassian.net/browse/${JIRA-ISSUE-NUMBER}
