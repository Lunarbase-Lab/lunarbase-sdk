#!/usr/bin/env bash
set -euo pipefail

: "${RELEASE_TAG:?RELEASE_TAG must be set}"
version="${RELEASE_TAG#v}"
if [[ "v${version}" != "${RELEASE_TAG}" ]]; then
  echo "RELEASE_TAG must be a v-prefixed version" >&2
  exit 2
fi

crate_list="$(
  node --input-type=module -e \
    'import { releaseCrates } from "./scripts/release-packages.mjs"; console.log(releaseCrates.map(({ name }) => name).join("\n"));'
)"
mapfile -t crates <<<"${crate_list}"
if [[ "${#crates[@]}" -ne 4 ]] || [[ -z "${crates[0]}" ]]; then
  echo "Expected exactly four release crates, found ${#crates[@]}" >&2
  exit 2
fi

cargo_target_directory="$(
  cargo metadata --no-deps --format-version 1 |
    node --input-type=module -e \
      'const chunks = []; for await (const chunk of process.stdin) chunks.push(chunk); process.stdout.write(JSON.parse(Buffer.concat(chunks)).target_directory);'
)"
local_archive=""
local_checksum=""

package_crate() {
  local crate="$1"
  cargo package --locked --no-verify --registry crates-io -p "${crate}"
  local_archive="${cargo_target_directory}/package/${crate}-${version}.crate"
  if [[ ! -f "${local_archive}" ]]; then
    echo "cargo package did not create ${local_archive}" >&2
    return 1
  fi
  local_checksum="$(node scripts/release-integrity.mjs sha256 "${local_archive}")"
}

refresh_local_checksum() {
  if [[ ! -f "${local_archive}" ]]; then
    echo "local crate archive disappeared: ${local_archive}" >&2
    return 1
  fi
  local_checksum="$(node scripts/release-integrity.mjs sha256 "${local_archive}")"
}

crate_matches_local() {
  local crate="$1"
  local response
  local response_body
  local response_status

  if ! response="$(
    curl --silent --show-error --retry 5 --retry-all-errors --retry-delay 2 \
      --header "Accept: application/json" \
      --header "User-Agent: lunarbase-release/1.0 (https://github.com/Lunarbase-Lab/lunarbase-sdk)" \
      --write-out $'\n%{http_code}' \
      "https://crates.io/api/v1/crates/${crate}/${version}"
  )"; then
    echo "crates.io lookup failed for ${crate}@${version}" >&2
    return 2
  fi

  response_status="${response##*$'\n'}"
  response_body="${response%$'\n'*}"
  case "${response_status}" in
    200)
      if printf '%s' "${response_body}" |
        node scripts/release-integrity.mjs verify-crate-response \
          "${crate}" "${version}" "${local_checksum}"; then
        return 0
      fi
      return 2
      ;;
    404) return 1 ;;
    *)
      echo "crates.io returned HTTP ${response_status} for ${crate}@${version}" >&2
      return 2
      ;;
  esac
}

wait_until_resolvable() {
  local crate="$1"
  local attempt
  local lookup_status
  for attempt in $(seq 1 30); do
    if crate_matches_local "${crate}"; then
      if cargo info "${crate}@${version}" --registry crates-io >/dev/null 2>&1; then
        return 0
      fi
    else
      lookup_status=$?
      if [[ "${lookup_status}" -eq 2 ]]; then
        return 1
      fi
    fi
    sleep 10
  done
  echo "${crate}@${version} was published but is not resolvable through the Cargo index" >&2
  return 1
}

for crate in "${crates[@]}"; do
  package_crate "${crate}"

  if crate_matches_local "${crate}"; then
    echo "${crate}@${version} already exists; waiting for Cargo index"
    wait_until_resolvable "${crate}"
    continue
  else
    lookup_status=$?
    if [[ "${lookup_status}" -eq 2 ]]; then
      exit 1
    fi
  fi

  published=false
  for attempt in $(seq 1 6); do
    if cargo publish --locked --registry crates-io -p "${crate}"; then
      refresh_local_checksum
      published=true
      break
    fi

    refresh_local_checksum
    if crate_matches_local "${crate}"; then
      echo "${crate}@${version} appeared after an ambiguous publish error"
      published=true
      break
    else
      lookup_status=$?
      if [[ "${lookup_status}" -eq 2 ]]; then
        exit 1
      fi
    fi

    if [[ "${attempt}" -lt 6 ]]; then
      sleep $((attempt * 10))
    fi
  done

  if [[ "${published}" != true ]]; then
    echo "Failed to publish ${crate}@${version} after six attempts" >&2
    exit 1
  fi

  wait_until_resolvable "${crate}"
done
