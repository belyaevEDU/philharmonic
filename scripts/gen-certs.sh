#!/usr/bin/env bash
# Generates a dev PKI for Philharmonic: a cluster CA plus
# manager, worker and client certificates signed by it.
#
# Usage: scripts/gen-certs.sh [output-dir] [options]
#        (output-dir defaults to ./certs)
#
# Options:
#   --manager-sans "SAN,..."   comma-separated SANs for the manager cert
#   --worker-sans  "SAN,..."   comma-separated SANs for the worker cert
#   --client-sans  "SAN,..."   comma-separated SANs for the client cert
# Each entry above is openssl-style: "DNS:hostname" or "IP:address". Default for all three:
# "DNS:localhost,IP:127.0.0.1".
#
#   --days-ca N                CA validity in days  (default: 3650)
#   --days-leaf N              node cert validity in days (default: 365)
#   -h, --help                 show this help
#
# IMPORTANT for external (non-localhost) nodes:
# TLS certificate verification matches the hostname/IP the *peer connects
# with* against the certificate's SANs. A worker reached at
# "worker.example.com:5556" needs "DNS:worker.example.com" in
# worker.tls/cert SANs; one reached at 10.0.0.6 needs "IP:10.0.0.6". Pass
# every name/address peers use, e.g.:
#
#   scripts/gen-certs.sh \
#     --manager-sans "DNS:mgmt.philharmonic.example,IP:10.0.0.5" \
#     --worker-sans  "DNS:worker.example.com,IP:10.0.0.6" \
#     --client-sans  "DNS:ops.laptop.example"
#
# and then use those exact addresses in manager.workers / client.manager.
#
# Each node certificate carries both serverAuth and clientAuth EKUs, so the
# same pair serves incoming and outgoing connections,
# matching how Philharmonic nodes use them
# (see manager.tls / worker.tls / client.tls in philharmonic.yaml).
#
# For anything beyond local development should probably use a real CA
# and shorter certificate lifetimes :shrug:

set -euo pipefail

DIR=""
DAYS_CA=3650
DAYS_LEAF=365
MANAGER_SANS="DNS:localhost,IP:127.0.0.1"
WORKER_SANS="DNS:localhost,IP:127.0.0.1"
CLIENT_SANS="DNS:localhost,IP:127.0.0.1"

usage() {
    # print this file's leading comment block as help text (skip shebang)
    awk 'NR > 1 && /^#/ { sub(/^# ?/, ""); print } NR > 1 && !/^#/ { exit }' "$0"
}

die() {
    echo "error: $*" >&2
    exit 1
}

# parse arguments: flags in any order, first positional is the output dir
while [[ $# -gt 0 ]]; do
    case "$1" in
        --manager-sans) [[ $# -ge 2 ]] || die "--manager-sans requires a value"; MANAGER_SANS="$2"; shift 2 ;;
        --worker-sans)  [[ $# -ge 2 ]] || die "--worker-sans requires a value"; WORKER_SANS="$2"; shift 2 ;;
        --client-sans)  [[ $# -ge 2 ]] || die "--client-sans requires a value"; CLIENT_SANS="$2"; shift 2 ;;
        --days-ca)      [[ $# -ge 2 ]] || die "--days-ca requires a value"; DAYS_CA="$2"; shift 2 ;;
        --days-leaf)    [[ $# -ge 2 ]] || die "--days-leaf requires a value"; DAYS_LEAF="$2"; shift 2 ;;
        -h|--help)      usage; exit 0 ;;
        -*)             die "unknown option: $1 (see --help)" ;;
        *)
            [[ -z "$DIR" ]] || die "unexpected extra argument: $1"
            DIR="$1"
            shift
            ;;
    esac
done
DIR="${DIR:-certs}"

[[ "$DAYS_CA" =~ ^[0-9]+$ ]]   && (( DAYS_CA > 0 ))   || die "--days-ca must be a positive integer, got '$DAYS_CA'"
[[ "$DAYS_LEAF" =~ ^[0-9]+$ ]] && (( DAYS_LEAF > 0 )) || die "--days-leaf must be a positive integer, got '$DAYS_LEAF'"

# validate_sans NAME "SAN,..." : each entry must be DNS:host or IP:address
validate_sans() {
    local name="$1" sans="$2"
    [[ -n "$sans" ]] || die "$name: SANs must not be empty"
    local -a entries
    IFS=',' read -r -a entries <<< "$sans"
    local entry
    for entry in "${entries[@]}"; do
        [[ "$entry" != *" "* ]] || die "$name: SAN entry '$entry' must not contain spaces"
        case "$entry" in
            DNS:*|IP:*) ;;
            *) die "$name: invalid SAN entry '$entry' (expected 'DNS:host' or 'IP:address', see --help)" ;;
        esac
    done
}

validate_sans "manager" "$MANAGER_SANS"
validate_sans "worker" "$WORKER_SANS"
validate_sans "client" "$CLIENT_SANS"

mkdir -p "$DIR"
umask 077

gen_leaf() { # name, SANs
    local name="$1" sans="$2"
    openssl req -new -newkey ec -pkeyopt ec_paramgen_curve:P-256 \
        -keyout "$DIR/$name.key" -out "$DIR/$name.csr" -nodes \
        -subj "/CN=$name" >/dev/null 2>&1
    openssl x509 -req -in "$DIR/$name.csr" \
        -CA "$DIR/ca.crt" -CAkey "$DIR/ca.key" -CAcreateserial \
        -out "$DIR/$name.crt" -days "$DAYS_LEAF" \
        -extfile <(printf 'subjectAltName=%s\nextendedKeyUsage=serverAuth,clientAuth\nkeyUsage=digitalSignature\n' "$sans") \
        >/dev/null 2>&1
    rm -f "$DIR/$name.csr"
    echo "wrote $DIR/$name.crt / $DIR/$name.key (SANs: $sans)"
}

if [[ ! -f "$DIR/ca.crt" ]]; then
    openssl req -x509 -new -newkey ec -pkeyopt ec_paramgen_curve:P-256 \
        -keyout "$DIR/ca.key" -out "$DIR/ca.crt" -days "$DAYS_CA" -nodes \
        -subj "/CN=philharmonic-ca" \
        -addext "basicConstraints=critical,CA:true" \
        -addext "keyUsage=critical,keyCertSign,cRLSign" >/dev/null 2>&1
    echo "wrote $DIR/ca.crt / $DIR/ca.key"
else
    echo "reusing existing $DIR/ca.crt (SAN changes do not re-sign: remove it to regenerate)"
fi

gen_leaf manager "$MANAGER_SANS"
gen_leaf worker  "$WORKER_SANS"
gen_leaf client  "$CLIENT_SANS"

echo
echo "SANs must match the addresses peers connect with. For local testing"
echo "that is DNS:localhost,IP:127.0.0.1 (the defaults); for external nodes"
echo "it is whatever appears in manager.workers / client.manager."
echo
cat <<EOF
example philharmonic.yaml settings (replace $DIR with the real path):
manager:
  tls:
    cert_file: $DIR/manager.crt
    key_file: $DIR/manager.key
    ca_file: $DIR/ca.crt
    client_ca_file: $DIR/ca.crt   # optional: require mTLS on the manager API
worker:
  tls:
    cert_file: $DIR/worker.crt
    key_file: $DIR/worker.key
    client_ca_file: $DIR/ca.crt   # only certificate holders may reach workers
client:
  tls:
    ca_file: $DIR/ca.crt
    cert_file: $DIR/client.crt    # needed for 'nodes' when workers use mTLS
    key_file: $DIR/client.key
EOF