# Pachyderm Technical Documentation Style Guide

This document provides main guidelines for creating technical content
that describes Pachyderm concepts and operations. This style guide is
based on Google Developer Documentation Style Guide and serves as a quick
reference for everyone who wants to contribute to the Pachyderm documentation.
For a more detailed overview, see the [Google Developer Documentation
Style Guide](https://developers.google.com/style/).

## Overview

We welcome all contributions to the Pachyderm technical documentation and are
happy to help incorporate your content into our docs! We hope that this
document will assits in answering some of your questions about our
contributing guidelines.

## Writing Style

Friendly but not overly colloquial. Avoid jargon, idioms, and references
to pop culture. Use shorter sentences and words over longer alternatives.

Things to avoid:

* "Please" and "thank you".
* Exclamation marks.
* Announcement of features that have not yet been developed.
* All caps to emphasize the importance.
* Parenthesis as much as possible. Use commas instead.

| Do not use | Use |
| ---------- | --- |
| We'll walk you through the installation of the product X. It might be a bit difficult, but don't worry, we are here to help. | This guide walks you through the process of installation of the product X. |

## Write in the present tense and in second person

Say "you" instead of "we" and use the present tense where possible. Only use
the future tense when talking about the events that will not happen immediately
but sometime in the future. The future tense introduces uncertainty about
when an action takes places. Therefore, in most cases, use the present tense.

| Do not use | Use |
| ---------- | --- |
| We are going to create a new configuration file that will describe our deployment. | To create a new configuration file that describes our deployment, complete the following steps. |

## Write for an international audience

Avoid idioms and jargon and write in simple American English. The content
that you are writing might later be translated into other foreign languages.
Translating simple short phrases is much easier than long sentences. Use
consistent terminology and avoid misplacing modifiers. Spell out abbreviations
on the first occurrence.

| Do not use | Use |
| ---------- | --- |
| After completing these steps, you are off to the races! | After you complete these steps, you can start using Pachyderm. |

## Write in Active Voice

Sentences written in active voice are easier for the reader to understand.
A well-written text has about 95% of sentences written in active voice.
Use passive voice only when the performer of the action is unknown or
to avoid blaming the user for an error.

| Do not use | Use |
| ---------- | --- |
| This behavior means that `transform.err_cmd` can be used to ignore failed datums. | You can use `transform.err_cmd` to ignore failed datums. |

## Put the Condition Before the Steps

If your sentence has a condition, start the sentence with the conditional
clause and add the descriptive instructions after the clause.

| Do not use | Use |
| ---------- | --- |
| See the Spark documentation for more information. | For more information, see the Spark documentation. |

## Use Numbered Lists for a Sequence of Steps

If the user needs to follow a set of instructions, organize them in a
numbered list rather than in a bulleted list. Options can be described in a
bulleted list. An exception to this rule is when you have just one step.

| Do not use | Use |
| ---------- | --- |
| * Create a configuration and run the following command. | 1. Create a configuration file. <br> 2. Run the following command: |

## Break Your Content Into Smaller Chunks

Users do not read the whole body of the text. Instead, they skip and
scan through looking for the text structures that stand out, such as
headings, numbered and bulleted lists, tables, and so on. Try to structure
your content so that it is easy to scan through by adding more titles,
organizing instructions in sequences of steps, and adding tables and
lists for properties and descriptions.

Avoid Ending a Sentence with a Preposition

Phrasal verbs are a little bit less formal than single-word verbs. If
possible, replace a phrasal word with a single-word verb equivalent and
if you have to use a phrasal word, avoid finishing the sentence with
a preposition.

!!! Info
    The following example is for illustration purposes only. 
    The sentence might not be relevant anymore
    depending on the version of Pachyderm considered.

| Do not use | Use |
| ---------- | --- |
| The `put file` API includes an option for splitting **up** the file into separate datums automatically. | The `put file` API includes an option for splitting the file into separate datums automatically. |

##Use meaningful links

Link text should mean something to the users when they read it. Phrases
like **Click here** and **Read more** do not provide useful information.
They might be good for call-to-action (CA) buttons on the marketing part
of the website, but in technical content they introduce uncertainty and
confusion.

Furthermore, if a user generates a list of links or uses a speech recognition
technology to navigate through the page, they use keywords and phrases,
such as "Click <text>". Generic links are not helpful for them.

Also, use a standard phrase *For more information, see <link>* to
introduce a link.

| Do not use | Use |
| ---------- | --- |
| More information about getting your FREE trial token and activating the dashboard can be found [here](https://docs.pachyderm.com/latest/enterprise/deployment/#activate-by-using-the-dashboard). | For more information, see [Activate your token by using the dashboard](https://docs.pachyderm.com/latest/enterprise/deployment/#activate-by-using-the-dashboard). |

## Markdown

The Pachyderm documentation uses Python Markdown, and many [PyMdown
Extensions](https://facelessuser.github.io/pymdown-extensions/) are supported.
See the `mkdocs.yaml` file for the list of supported
extensions.

In general, follow these guidelines:

* Do not use the admonitions in numbered lists because they break the order
and Markdown does not support starting lists from an arbitrary number.

* Enclose code blocks in "```" and specify the correct highlighting.

* While PyMdown Extensions provide advanced UI features, use them sparingly because not all browsers fully support all of them.

* Do not use the dollar sign `$` in code snippets. While the dollar sign
signifies the beginning of a line, it creates usability issues. When users
copy commands from a code block, they copy everything from that code
block, including the dollar sign. Then, they need to navigate to the
begining of the command prompt to remove the dollar sign, which is quite annoying.
Most of the commands that are described in the Pachyderm documentation
are run from a UNIX shell as a normal user.

## Preview the Documentation Locally

The documentation for each version is stored in eponymous folders. Each
version uses its own configuration file that has a suffix named after
the corresponding version.

To serve the documentation locally, you need to run the `mkdocs serve`
command with the corresponding configuration file from the `docs`
folder.

For example, to build the master version of documentation, run:

```shell
mkdocs serve -f mkdocs-master.yml
```

## Generating the Go API Documentation

We use `godoc` to generate Go API documentation. It is automatically
published at godoc.org. If you are adding something new, such as
an example, and want to check how it looks in HTML, you can build the
documentation locally. To install `godoc`, run the following command:

```shell
go get golang.org/x/tools/cmd/godoc
```

To build the documentation, run the following
command from the root directory of the `pachyderm` repository:

```shell
godoc -http=:6060 -goroot=$(pwd)
```

To view the documentation, open `localhost:6060/pkg` in a web browser.
Most of the Pachyderm API documentation, can be found under the
`client` directory.

I hope you'll have fun with Python Markdown! :smile:

## Change log

Format release notes as a bulleted list with each bullet describing a new
feature, bug fix, or change. Each bullet point should contain at most a few sentences,
and the vast majority should be just one sentence that describes the change. Sentences
must be written in the present tense and the issues that were fixed past tense. For
example:

| Do not use | Use |
| ---------- | --- |
| Fixed a bug which causes ... | Fixes a bug which caused ... |

It is helpful to imagine the name of the release, such as *Pachyderm 1.11.0*,
prepended to each change in the log.

Use the following verbs for different types of changes:

| Use | Verb |
| --- | ---- |
| Bug fix | Fixes |
| New Feature | Adds |
| Version update of dependency | Updates |
| Generic change | Changes |

Almost every line in our changelog starts with one of these words. However,
there are rare cases where it's awkward to explain the change using this
structure in which case it's fine to ignore it. However if you find yourself
using synonyms like "Upgrade" those should be replaced with words from the
above table.

Include the PR(s) that merged the change at the end of the line in parenthesis,
i.e. (#123). The # is significant because github will turn that into a link.
Don't include the issue that was closed by the change, that should be linked to
in the PR. If the change was contributed by an external contributor add
"thanks to @user" i.e. "(#123, thanks to @user)"

Release notes are published in two places: CHANGELOG.md in this repo, and the
github release page. Make sure to update both when you publish change logs.
