# Releasing

Releases go to Maven Central through the [Central Portal](https://central.sonatype.com). Pushing a
`v*.*.*` tag runs `.github/workflows/release.yml`, which sets the version from the tag, runs the full
build and test suite, signs, and uploads. The upload is validated and then **held**: nothing is public
until someone presses Publish in the portal. A published release can never be changed or deleted.

## One-time setup

1. **Central Portal account.** Sign in at <https://central.sonatype.com>.
2. **Namespace.** Under *Namespaces*, add `se.docksidelabs`. The portal shows a verification key; add
   it as a DNS `TXT` record on `docksidelabs.se`, then press *Verify* once `dig TXT docksidelabs.se`
   shows it.
3. **Portal token.** Under *Account*, generate a user token. Store its two halves as repository
   secrets:

   ```sh
   gh secret set MAVEN_CENTRAL_USERNAME
   gh secret set MAVEN_CENTRAL_PASSWORD
   ```

4. **Signing key.** Central requires every file to be signed with a key published on a public
   keyserver.

   ```sh
   brew install gnupg
   gpg --quick-gen-key "Robin Börjesson (pg-notify release signing)" ed25519 sign 3y
   gpg --list-secret-keys --keyid-format long          # note the fingerprint
   gpg --keyserver keyserver.ubuntu.com --send-keys <FINGERPRINT>

   gpg --armor --export-secret-keys <FINGERPRINT> | gh secret set MAVEN_GPG_KEY
   gh secret set MAVEN_GPG_PASSPHRASE                  # the passphrase chosen above
   ```

   Keep an offline backup of the key. Releases signed with a lost key stay valid, but a new key means
   users who pinned the old one see a change.

## Each release

1. `main` is green in CI.
2. Tag and push: `git tag v0.1.0 && git push origin v0.1.0`.
3. When the workflow finishes, open *Deployments* in the portal, check the files, and press
   *Publish*. It reaches Maven Central within about half an hour.
4. On `main`, set the next development version (`mvn versions:set -DnewVersion=0.2.0-SNAPSHOT
   -DgenerateBackupPoms=false`) and update the dependency snippet in the README.

The version in `pom.xml` on `main` stays a `-SNAPSHOT`; the tag decides the released version. The
`release` profile refuses to run on a SNAPSHOT, so an untagged build can never be uploaded.
