// Two small conveniences. The page is complete without either of them.

// Copy an install command.
for (const button of document.querySelectorAll(".copy")) {
  button.addEventListener("click", async () => {
    const source = document.querySelector(button.dataset.copy);
    if (!source) return;
    try {
      await navigator.clipboard.writeText(source.textContent.trim());
      const label = button.textContent;
      button.textContent = "Copied";
      button.dataset.copied = "";
      setTimeout(() => {
        button.textContent = label;
        delete button.dataset.copied;
      }, 1600);
    } catch {
      // Clipboard blocked (no permission, insecure origin): leave the text
      // on screen for the reader to select by hand.
    }
  });
}

// Switch the screenshot showcase.
const panel = document.querySelector("[data-shot-panel]");
const captions = {
  browser:
    "Keys as a folder tree, split by ':'. TTLs count down in place, and a JSON value is formatted and coloured as you read it.",
  connections:
    "Saved profiles, each showing what it is: TLS, a keychain password, an SSH tunnel, and the read-only flag that refuses every write.",
  "server-info":
    "INFO in sections, with a usage bar against maxmemory, plus the slow log, client list and running config.",
  memory:
    "Which prefix is holding the RAM, measured rather than guessed, with the share of the keyspace behind each answer.",
  pubsub:
    "A live feed with a rate sparkline over the last minute, a per-channel breakdown, and the selected message pretty-printed.",
  editor:
    "Editing a value. JSON is checked before it is saved, so a typo is refused rather than stored.",
};

if (panel) {
  const image = panel.querySelector("img");
  const caption = panel.querySelector("figcaption");
  for (const tab of document.querySelectorAll(".tabs button")) {
    tab.addEventListener("click", () => {
      const name = tab.dataset.shot;
      for (const other of document.querySelectorAll(".tabs button")) {
        other.setAttribute("aria-selected", String(other === tab));
      }
      image.src = `screenshots/${name}.svg`;
      image.alt = tab.textContent;
      caption.textContent = captions[name] ?? "";
    });
  }
}
