function runTariffUnitTests() {
  assertTariffEquals_('gmail address extraction', 'rates@example.com', extractEmails_(
    'Rates Team <rates@example.com>'
  )[0]);
  assertTariffEquals_('domain email rule', true, matchesEmailRule_(
    'rates@provider.com',
    '@provider.com'
  ));
  assertTariffEquals_('comma decimal rate', 0.045, parseRate_('0,045', {}));
  assertTariffEquals_('mixed separator rate', 1234.56, parseRate_('1.234,56', {}));
  assertTariffEquals_(
    'accent normalization',
    'pais movil',
    normalizeTextKey_('Pa\u00eds M\u00f3vil')
  );
  assertTariffEquals_('network code formatting', '01', normalizeNetworkCode_('01'));
  assertTariffEquals_('dataset purchase alias', 'costos', normalizeDatasetType_('Compras'));
  testTariffRowNormalization_();
  console.log('All tariff unit tests passed.');
}

function testTariffRowNormalization_() {
  const routeCatalog = {
    byAlias: {
      'provider_a|chile entel mobile': {
        country: 'Chile',
        route: 'Chile Mobile',
        mcc: '730',
        mnc: '01',
        operator: 'Entel'
      }
    },
    byNetwork: {}
  };
  const record = normalizeParsedRow_({
    rowNumber: 2,
    headers: ['Destination', 'Price'],
    values: {
      Destination: 'Chile Entel Mobile',
      Price: '0,045'
    }
  }, {
    actor: {
      id: 'provider_a',
      name: 'Provider A',
      datasetType: 'costos',
      defaultCurrency: 'USD'
    },
    template: {
      columnMap: {
        route: ['Destination'],
        rate: ['Price']
      },
      transforms: {}
    },
    routeCatalog: routeCatalog,
    messageId: 'message-1',
    messageAt: new Date('2026-06-10T10:00:00Z'),
    attachmentName: 'rates.csv',
    attachmentHash: 'hash-1'
  });

  assertTariffEquals_('canonical route', 'Chile Mobile', record.route);
  assertTariffEquals_('catalog MCC', '730', record.mcc);
  assertTariffEquals_('catalog MNC', '01', record.mnc);
  assertTariffEquals_('normalized rate', 0.045, record.rate);
  assertTariffEquals_(
    'record key',
    'costos|provider_a|chile mobile|730|01',
    record.recordKey
  );
}

function assertTariffEquals_(name, expected, actual) {
  if (expected !== actual) {
    throw new Error(
      name + ' failed. Expected ' + JSON.stringify(expected) +
      ', received ' + JSON.stringify(actual) + '.'
    );
  }
}
