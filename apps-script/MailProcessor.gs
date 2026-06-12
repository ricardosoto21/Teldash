function scanTariffMailbox() {
  const lock = LockService.getScriptLock();
  if (!lock.tryLock(1000)) {
    console.log('Another tariff scan is already running.');
    return;
  }

  try {
    const spreadsheet = getTariffSpreadsheet_();
    ensureSchema_(spreadsheet, false);
    const configuration = readConfiguration_(spreadsheet);
    const processedKeys = loadProcessedImportKeys_(spreadsheet);
    const runtime = getRuntimeOptions_();
    const state = {
      spreadsheet: spreadsheet,
      configuration: configuration,
      processedKeys: processedKeys,
      seenMessages: {},
      attachmentsProcessed: 0,
      maxAttachments: runtime.maxAttachments
    };

    processMailboxSearch_(
      'in:inbox has:attachment newer_than:' + runtime.scanDays + 'd',
      'received',
      runtime.maxThreads,
      state
    );
    if (state.attachmentsProcessed < state.maxAttachments) {
      processMailboxSearch_(
        'in:sent has:attachment newer_than:' + runtime.scanDays + 'd',
        'sent',
        runtime.maxThreads,
        state
      );
    }
    console.log('Tariff scan completed. Attachments inspected: ' + state.attachmentsProcessed);
  } finally {
    lock.releaseLock();
  }
}

function processMailboxSearch_(query, mailboxSide, maxThreads, state) {
  const threads = GmailApp.search(query, 0, maxThreads);
  for (let threadIndex = 0; threadIndex < threads.length; threadIndex += 1) {
    const messages = threads[threadIndex].getMessages();
    for (let messageIndex = 0; messageIndex < messages.length; messageIndex += 1) {
      if (state.attachmentsProcessed >= state.maxAttachments) {
        return;
      }

      const message = messages[messageIndex];
      if (state.seenMessages[mailboxSide + '|' + message.getId()]) {
        continue;
      }
      state.seenMessages[mailboxSide + '|' + message.getId()] = true;
      processMessage_(message, mailboxSide, state, false);
    }
  }
}

function reprocessTariffMessage(messageId, mailboxSide) {
  const side = normalizeMailboxSide_(mailboxSide);
  if (!side || side === 'both') {
    throw new Error('mailboxSide must be "received" or "sent".');
  }

  const spreadsheet = getTariffSpreadsheet_();
  ensureSchema_(spreadsheet, false);
  const configuration = readConfiguration_(spreadsheet);
  const state = {
    spreadsheet: spreadsheet,
    configuration: configuration,
    processedKeys: {},
    seenMessages: {},
    attachmentsProcessed: 0,
    maxAttachments: 100
  };
  processMessage_(GmailApp.getMessageById(messageId), side, state, true);
}

function processMessage_(message, mailboxSide, state, force) {
  const actorCandidates = getMessageActorCandidates_(
    message,
    mailboxSide,
    state.configuration.actors
  );
  if (!actorCandidates.length) {
    return;
  }

  const attachments = message.getAttachments({
    includeInlineImages: false,
    includeAttachments: true
  });
  if (!attachments.length) {
    return;
  }

  for (let index = 0; index < attachments.length; index += 1) {
    if (!force && state.attachmentsProcessed >= state.maxAttachments) {
      return;
    }
    state.attachmentsProcessed += 1;
    processAttachment_(message, mailboxSide, attachments[index], state, force);
  }
}

function processAttachment_(message, mailboxSide, attachment, state, force) {
  const messageId = message.getId();
  const messageAt = message.getDate();
  const attachmentName = attachment.getName();
  const attachmentHash = hashAttachment_(attachment);
  const importKey = importKey_(messageId, attachmentName, attachmentHash);
  if (!force && state.processedKeys[importKey]) {
    return;
  }

  let actor = null;
  let template = null;
  try {
    actor = resolveActorForAttachment_(
      message,
      mailboxSide,
      attachmentName,
      state.configuration
    );
    template = state.configuration.templates.find(function(candidate) {
      return candidate.id === actor.parserId;
    });
    if (!template) {
      throw tariffError_(
        'PARSER_NOT_FOUND',
        'Parser "' + actor.parserId + '" is not configured or enabled.'
      );
    }

    const parsedRows = parseAttachment_(attachment, template);
    const normalized = normalizeParsedRows_(parsedRows, {
      actor: actor,
      template: template,
      routeCatalog: state.configuration.routeCatalog,
      messageId: messageId,
      messageAt: messageAt,
      attachmentName: attachmentName,
      attachmentHash: attachmentHash
    });
    appendErrors_(state.spreadsheet, normalized.errors);
    const published = upsertRates_(state.spreadsheet, normalized.valid);
    const status = normalized.valid.length === 0
      ? 'REJECTED'
      : (normalized.errors.length ? 'PARTIAL' : 'SUCCESS');

    appendHistory_(state.spreadsheet, {
      messageId: messageId,
      messageAt: messageAt,
      mailboxSide: mailboxSide,
      actorId: actor.id,
      actorName: actor.name,
      datasetType: actor.datasetType,
      attachmentName: attachmentName,
      attachmentHash: attachmentHash,
      status: status,
      rowsRead: parsedRows.length,
      rowsPublished: published,
      rowsRejected: normalized.errors.length,
      details: published + ' current rates written.'
    });
    state.processedKeys[importKey] = true;
  } catch (error) {
    const code = error.code || 'IMPORT_ERROR';
    const status = code === 'UNSUPPORTED_FILE_TYPE' ? 'UNSUPPORTED' : 'ERROR';
    appendErrors_(state.spreadsheet, [{
      messageId: messageId,
      actorId: actor ? actor.id : '',
      attachmentName: attachmentName,
      rowNumber: '',
      code: code,
      message: error.message,
      raw: {
        from: message.getFrom(),
        to: message.getTo(),
        subject: message.getSubject()
      }
    }]);
    appendHistory_(state.spreadsheet, {
      messageId: messageId,
      messageAt: messageAt,
      mailboxSide: mailboxSide,
      actorId: actor ? actor.id : '',
      actorName: actor ? actor.name : '',
      datasetType: actor ? actor.datasetType : '',
      attachmentName: attachmentName,
      attachmentHash: attachmentHash,
      status: status,
      rowsRead: 0,
      rowsPublished: 0,
      rowsRejected: 1,
      details: code + ': ' + error.message
    });
    state.processedKeys[importKey] = true;
    console.error('Tariff import failed for ' + attachmentName + ': ' + error.message);
  }
}

function resolveActorForAttachment_(message, mailboxSide, attachmentName, configuration) {
  let candidates = getMessageActorCandidates_(
    message,
    mailboxSide,
    configuration.actors
  );

  if (candidates.length > 1) {
    candidates = candidates.filter(function(actor) {
      const template = configuration.templates.find(function(item) {
        return item.id === actor.parserId;
      });
      return template && filenameMatches_(attachmentName, template.filenameRegex);
    });
  }

  if (!candidates.length) {
    throw tariffError_(
      'ACTOR_NOT_FOUND',
      'No enabled actor matches the message addresses for ' + mailboxSide + '.'
    );
  }
  if (candidates.length > 1) {
    throw tariffError_(
      'AMBIGUOUS_ACTOR',
      'More than one actor matches this message and attachment.'
    );
  }
  return candidates[0];
}

function getMessageActorCandidates_(message, mailboxSide, actors) {
  const addressHeader = mailboxSide === 'received'
    ? message.getFrom()
    : [message.getTo(), message.getCc(), message.getBcc()].join(',');
  const addresses = extractEmails_(addressHeader);
  return actors.filter(function(actor) {
    return (actor.mailboxSide === mailboxSide || actor.mailboxSide === 'both') &&
      actor.emailRules.some(function(rule) {
        return addresses.some(function(address) {
          return matchesEmailRule_(address, rule);
        });
      });
  });
}

function extractEmails_(header) {
  const matches = String(header || '').toLowerCase().match(
    /[a-z0-9.!#$%&'*+/=?^_`{|}~-]+@[a-z0-9.-]+\.[a-z]{2,}/g
  );
  return matches || [];
}

function matchesEmailRule_(email, rule) {
  const normalizedEmail = String(email || '').trim().toLowerCase();
  const normalizedRule = String(rule || '').trim().toLowerCase();
  if (!normalizedRule) {
    return false;
  }
  if (normalizedRule.indexOf('@') === 0) {
    return normalizedEmail.endsWith(normalizedRule);
  }
  if (normalizedRule.indexOf('*@') === 0) {
    return normalizedEmail.endsWith(normalizedRule.slice(1));
  }
  return normalizedEmail === normalizedRule;
}

function hashAttachment_(attachment) {
  const digest = Utilities.computeDigest(
    Utilities.DigestAlgorithm.SHA_256,
    attachment.getBytes()
  );
  return Utilities.base64EncodeWebSafe(digest).replace(/=+$/, '');
}

function getRuntimeOptions_() {
  const properties = PropertiesService.getScriptProperties();
  return {
    scanDays: positiveInteger_(
      properties.getProperty(TARIFF_CONFIG.SCAN_DAYS_PROPERTY),
      TARIFF_CONFIG.DEFAULT_SCAN_DAYS
    ),
    maxThreads: positiveInteger_(
      properties.getProperty(TARIFF_CONFIG.MAX_THREADS_PROPERTY),
      TARIFF_CONFIG.DEFAULT_MAX_THREADS
    ),
    maxAttachments: positiveInteger_(
      properties.getProperty(TARIFF_CONFIG.MAX_ATTACHMENTS_PROPERTY),
      TARIFF_CONFIG.DEFAULT_MAX_ATTACHMENTS
    )
  };
}
