# Release procedure

## Types of releases

|ReleaseType|Example Version|Built off master|Can build off any branch| Updates docs| Can host multiple install versions |
|---|---|---|---|---|---|
|Point Release| v1.7.2| Y | N | Y | N |
|Release Candidate| v1.8.0rc1 | Y | N | Y | N |
|Custom Release | v1.8.1-aeeff234982735987affee | N | Y | N | Y |

## Requirements

NOTE! At the moment, we require the release script to be run on an ubuntu
machine.

This is because of a dependency on CGO via
[this bug](https://github.com/opencontainers/runc/issues/841), as we don't
want to enable CGO in part because it doesn't play nice with macOS for us.

You'll need the following credentials / tools:

- A GitHub *Personal Access Token* with **repo** access
  - You can get your personal oauth token here: https://github.com/settings/tokens
- Add your GITHUB token as env variable in your profile. This is required by goreleaser
  - Eg. in ~/.bash_profile add the following line `export GITHUB_TOKEN="YOUR-GH-TOKEN"`
- access to `homebrew-tap` and `www` repositories
- S3 credentials
- A dockerhub account, with write access to
  [pachyderm](https://hub.docker.com/u/pachyderm/) (run `docker login`)
- `goreleaser`
    - on linux: 
    ```bash
    pushd /usr/local/
    curl -sfL https://install.goreleaser.com/github.com/goreleaser/goreleaser.sh | sudo sh
    popd
    ```
    - on mac: `brew install goreleaser/tap/goreleaser`
- `silversearcher`
    - on linux: `apt-get install -y silversearcher-ag`
    - on mac: `brew install the_silver_searcher`

If you're doing a custom release (off a branch that isn't master),
[skip to the section at the bottom](#custom-releases)

## Releasing

The following is the procedure for point releases, rcs, or anything else off
of master; i.e. for non-custom releases. For custom release instructions, see
below.

### Prerequisites

1. Make sure the HEAD commit (that you're about to release) has a passing
   build on circleci.
2. Make sure that you have no uncommitted files in the current branch.

### Update client version [apply step only when running point-release target]

Update `src/version/client.go` version values.

- for a major release, increment the MajorVersion and set the MinorVersion and
  MicroVersion to 0; e.g. `2.0.0`.
- for a minor release, leave the MajorVersion unchanged, increment the
  MinorVersion, and set the MicroVersion to 0; e.g. `1.10.0`.
- for a patch release, leave the MajorVersion and MinorVersion unchanged and
  increment the MicroVersion; e.g. `1.9.8`.

Commit these changes locally (you will push to GitHub in a later step):

```shell
make VERSION_ADDITIONAL= install
git add src/version/client.go
git commit -m"Increment version for $(pachctl version --client-only) release"
```

### Update the Pachyderm version in the Helm Chart

`etc/helm/pachyderm/Chart.yaml`

Update the `appVersion` key to the new pachyderm version

Update the `version` key to the new pachyderm version (Helm and pachyderm versions will be kept in lock step)

Note: When releasing an alpha/beta/RC version, ensure the helmchart is marked as a pre-release

`etc/helm/pachyderm/Chart.yaml`

```
annotations:
  artifacthub.io/prerelease: "true"
```

This will ensure the release is marked as a pre-release on artifact hub

Commit your change to the repo:

```git commit -am "Update Pachyderm version in helm for <new pachyderm version> release"```

### Update testfaster commit hash [apply step only when running point-release target]

When changing the version number, we have to update the version of various built assets. These assets are tagged with the version number which must correspond to the version that the tests try to use. In particular, [these docker images](https://github.com/pachyderm/pachyderm/blob/442f8e119c75c3f183f228dd0fcfb7f67b6c0798/.testfaster.yml#L92-L98) are version-tagged and cached in Testfaster. This means that to get the tests to work, the cached version number (e.g. `1.12.2`) must match the version number self-reported by `pachctl version --client-only`.

**Related note:** If you modify the contents of those make targets and wonder why they're not changing, you'll also need to bump the hash (and wait for testfaster to build the base image next time you push) to get those changes to be reflected in CI.

Find the latest commit hash:

```shell
git log --pretty=format:%H | head -n 1
```

Edit `.testfaster.yml` and edit the line that looks like [this](https://github.com/pachyderm/pachyderm/blob/442f8e119c75c3f183f228dd0fcfb7f67b6c0798/.testfaster.yml#L63) which specifies the commit hash that is included in the base image for the test VMs.

In particular, update the line which includes `git checkout`.

Update the hash to the one above, and make a new commit:

```shell
git commit -am "Update testfaster hash for $(pachctl version --client-only) release"
```

Note that most times that you push changes to `.testfaster.yml`, Testfaster will automatically rebuild the base image. This can take some time, but further usage of that base image should be fast again. It's also possible that these base image builds can fail due to transient network issues, in that case go to the [Testfaster UI](https://testfaster.ci/pools) and hit the retry button for the pool in question (corresponding to the branch name). Note that at time of writing, you need to manually refresh the "pool logs" page to get new logs.

### Update the changelog [apply step only when running point-release target]

Update the changelog in the branch and commit it locally. Edit `CHANGELOG.md`

```shell
git commit -am "Update change log for $(pachctl version --client-only) release"
```

Note: The changelog must be the last commit to be properly parsed by `etc/build/make_changelog.sh`

### Push changes [apply step only when running point-release target]

In a typical point release you will have 3 commits to push to the server.

```shell
git push
```

### Release! [apply step only when running point-release or release-candidate target]

* To release a major, minor, or patch version, run
```shell
make point-release
```
* To release an alpha/beta/RC version, specify the additional text to appear in the release version and run
```shell
make VERSION_ADDITIONAL=-alpha1 release-candidate
OR
make VERSION_ADDITIONAL=-beta1 release-candidate
OR
make VERSION_ADDITIONAL=-rc1 release-candidate
 ```

### Release notes [apply step only when running point-release target]
* [Release notes](https://github.com/pachyderm/pachyderm/releases) are automatically
updated in GitHub. These are pulled from `CHANGELOG.md`. Check to make sure the notes
are correct. Edit the release on GitHub to manually update any changes.

* Post update in the #users channel with the following template
```shell
@here Hi All,
    We’ve just released Pachyderm <X.Y.Z> — check it out!
    * RELEASE NOTES with links to PRs
```

### Helm
The helm chart will be released when the release tag is pushed to the repo. 

### New major or minor releases

In the case of a new major or minor release (x.0.0 or 1.x.0), you will need
to make a couple of additional changes. See
[this document](./major_minor_release_instructions.md) for details.

## Custom releases

Occasionally we have a need for a custom release off a non-master branch. This
is usually because some features we need to supply to users that are
incompatible with features on master, but the features on master we need to
keep longer-term.

Assuming the prerequisites are met, making a custom release should simply be a
matter of running `make custom-release`. This will create a release like
`v1.2.3-2342345aefda9879e87ad`, which can be installed like:

```shell
curl -o /tmp/pachctl.deb -L https://github.com/pachyderm/pachyderm/releases/download/v1.2.3-2342345aefda9879e87ad/pachctl_1.2.3-2342345aefda9879e87ad_amd64.deb && sudo dpkg -i /tmp/pachctl.deb
```

Or for mac/brew:

```shell
# Where 1.2 is the major.minor version of the release you just did,
# and you use the right commit SHA as well in the URL
brew install https://raw.githubusercontent.com/pachyderm/homebrew-tap/1.2.3-2342345aefda9879e87ad/pachctl@1.2.rb
```

## If the release failed

You'll need to delete the *release* and the *release tag* in github. Navigate
to the [pachyderm repo](https://www.github.com/pachyderm/pachyderm) and click
on the *Releases* tab. Click on the big, blue version number corresponding to
the release you want to delete, and you should be redirected to a page with
just that release, and red "Delete" button on the top right. Click the delete
button.

From here, go back to the list of Pachyderm releases, and click "tags". Click
on the tag for the release you want to delete, and then click "delete" again
to delete the tag.

At this point, you can re-run the release process when you're ready.

## Rolling back a release

If a release has a problem and needs to be withdrawn, the steps in rolling
back a release are similar to the steps under "If the release failed". In
general, you'll need to:
- Delete the tag and GitHub Release for both the bad release *and the most
  recent good release*
- Re-release the previous version (to update homebrew)

All of these can be accomplished by:
- Following the steps under "If the release failed" for deleting the tag and
  GitHub release for both the bad release
- Checking out the git commit associated with the most recent good release
  (`git checkout tags/v<good release>`). Save this commit SHA
  (`git rev-list tags/v<good> --max-count=1`), in case you need it later, as
  we'll be deleting the tag.
- Delete the tag and GitHub release for the last good release (the one you
  just checked out)
- Syncing your local Git tags with the set of tags on Github (either re-clone
  the Pachyderm repo, or run
  `git tag -l | xargs git tag -d; git fetch origin master --tags`). This
  prevents the release process from failing with `tag already exists`.
- Run `make point-release` (or follow the release process for custom releases)

Helm
- Delete the release from the https://github.com/pachyderm/helmchart repo
- Rollback the commit on the gh-pages branch which added the release to the index.yaml 
in https://github.com/pachyderm/helmchart
