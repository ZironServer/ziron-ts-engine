import {
  analyseTypeofData,
  BadConnectionError,
  BadConnectionType,
  DataType, DynamicGroupTransport, GroupTransport, InsufficientBufferSizeError,
  isBatched,
  JSONString,
  Package,
  PackageBuffer,
  ReadStream,
  StreamCloseError,
  StreamCloseCode,
  StreamState,
  TimeoutError,
  Transport,
  WriteStream
} from './../src/index';
import * as chai from 'chai';
import {expect} from 'chai';
import * as chaiAsPromised from 'chai-as-promised';
import LinkedBuffer from "../src/lib/streams/LinkedBuffer";

chai.use(chaiAsPromised);

const comA1 = new Transport({
  onInvalidMessage: (err) => console.error('A1: Invalid meessage: ', err),
  onListenerError: (err) => console.error('A1: Listener err: ', err),
},{...Transport.DEFAULT_OPTIONS},true);
const comB1 = new Transport({
  onInvalidMessage: (err) => console.error('B1: Invalid meessage: ', err),
  onListenerError: (err) => console.error('B1: Listener err: ', err),
},{...Transport.DEFAULT_OPTIONS},true);
const comA2 = new Transport({
  onInvalidMessage: (err) => console.error('A2: Invalid meessage: ', err),
  onListenerError: (err) => console.error('A2: Listener err: ', err),
},{...Transport.DEFAULT_OPTIONS},true);
const comB2 = new Transport({
  onInvalidMessage: (err) => console.error('B2: Invalid meessage: ', err),
  onListenerError: (err) => console.error('B2: Listener err: ', err),
},{...Transport.DEFAULT_OPTIONS},true);

const comBGroup = new GroupTransport({
  send: (msg) => {
    comB1.emitMessage(msg);
    comB2.emitMessage(msg);
  },
  isConnected: () => comB1.open && comB2.open
});

const dynamicGroup = new DynamicGroupTransport({
  send: ((group, msg) => {
    if(group === 'A') {
      comA1.emitMessage(msg);
      comA2.emitMessage(msg);
    }
    else if(group === "B"){
      comB1.emitMessage(msg);
      comB2.emitMessage(msg);
    }
  }),
  isConnected: () => true
});

//connect
comA1.connect(comB1);
comA2.connect(comB2);

function concatenateBuffer(...arrays: ArrayBuffer[]): ArrayBuffer {
  const size = arrays.reduce((a,b) => a + b.byteLength, 0)
  const result = new Uint8Array(size)
  let offset = 0
  for (const arr of arrays) {
    result.set(arr as any, offset)
    offset += arr.byteLength
  }
  return result.buffer;
}

function generateArray(size: number,generator: (index: number) => any): any[] {
  const array: any[] = [];
  for(let i = 0; i < size; i++) array.push(generator(i));
  return array;
}

function randomIntFromInterval(min, max) {
  return Math.floor(Math.random() * (max - min + 1) + min)
}

describe('Ziron', () => {

  afterEach(() => {
    [comA1,comA2,comB1,comB2].forEach((com) => {
      com.emitConnection();
      com.hasLowSendBackpressure = () => true;
      com.onTransmit = () => {};
      com.onInvoke = () => {};
      com.options.limitBatchStringLength = 310000;
      com.options.limitBatchBinarySize = 3145728;
      com.options.maxBufferChunkLength = 200;
      com.options.maxBufferSize = Number.POSITIVE_INFINITY;
    });
    comBGroup.options.limitBatchStringLength = 310000;
    dynamicGroup.options.limitBatchStringLength = 310000;
    dynamicGroup.options.freeBufferMaxPoolSize = 100;
  })

  describe('Transmits', () => {

    [
      {
        title: 'B should receive the transmit with JSON data.',
        data: {name: 'Luca', age: 21},
        expectedData: {name: 'Luca', age: 21},
        expectedDataType: DataType.JSON,
        processComplexTypes: false
      },
      {
        title: 'Transmit with circular JSON data should be handled.',
        data: (() => {
          const data: any = {};
          data.more = data;
          return data;
        })(),
        expectedData: {more: '[Circular]'},
        expectedDataType: DataType.JSON,
        processComplexTypes: false
      },
      {
        title: 'B should receive the transmit with binary data.',
        data: new ArrayBuffer(10),
        expectedData: new ArrayBuffer(10),
        expectedDataType: DataType.Binary,
        processComplexTypes: true
      },
      {
        title: 'B should receive the transmit with quotes in the receiver name.',
        receiver: 'Send"Message',
        data: "Hello World!",
        expectedData: "Hello World!",
        expectedDataType: DataType.JSON,
        processComplexTypes: false
      },
      {
        title: 'B should receive the transmit with JSON string.',
        data: new JSONString('[]'),
        expectedData: [],
        expectedDataType: DataType.JSON,
        processComplexTypes: false
      },
      {
        title: 'B should receive the transmit with MixedJSON (JSON with binary).',
        data: {avatar: new ArrayBuffer(5),cover: new ArrayBuffer(15)},
        expectedData: {avatar: new ArrayBuffer(5),cover: new ArrayBuffer(15)},
        expectedDataType: DataType.JSONWithBinaries,
        processComplexTypes: true
      },
      {
        title: 'B should receive the transmit with MixedJSON (JSON with binary) and internal special keys (_b and _s).',
        data: {_b: new ArrayBuffer(5),cover: new ArrayBuffer(15),_s: 10,test: {_b: 10}},
        expectedData: {_b: new ArrayBuffer(5),cover: new ArrayBuffer(15),_s: 10,test: {_b: 10}},
        expectedDataType: DataType.JSONWithBinaries,
        processComplexTypes: true
      }
    ].forEach(test => {
      it(test.title, async () => {
        const receiver = test.receiver || 'someReceiver';
        const receivePromise = new Promise((res,rej) => {
          comB1.onTransmit = (inReceiver,data,type) => {
            try {
              expect(inReceiver).to.be.equal(receiver);
              expect(data).to.be.deep.equal(test.expectedData);
              expect(type).to.be.equal(test.expectedDataType);
              res();
            }
            catch(e) {rej(e)}
          };
        });
        comA1.transmit(receiver, test.data, {processComplexTypes: test.processComplexTypes});
        await receivePromise;
      });
    })
  });

  describe('Multi Transmits', () => {

    [
      {
        title: 'B1 and B2 should receive the multi transmit with JSON data.',
        data: {name: 'Luca', age: 21},
        processComplexTypes: false
      },
      {
        title: 'B1 and B2 should receive the multi transmit with quotes in the receiver name.',
        receiver: 'Send"Message',
        data: "Hello World!",
        processComplexTypes: false
      },
      {
        title: 'B1 and B2 should receive the multi transmit with MixedJSON (JSON with binary) 1.',
        data: {numbers: [24,24,2,35,35], image: new ArrayBuffer(100)},
        processComplexTypes: true
      },
      {
        title: 'B1 and B2 should receive the multi transmit with MixedJSON (JSON with binary) 2.',
        data: {image: new ArrayBuffer(50), cover: new ArrayBuffer(100)},
        processComplexTypes: true
      },
      {
        title: 'B1 and B2 should receive the multi transmit with binary data.',
        data: new ArrayBuffer(100),
        processComplexTypes: true
      }
    ]
    .forEach(test => {
      it(test.title, async () => {
        const receiver = test.receiver || 'someReceiver';
        const receivePromise = Promise.all([comB1,comB2].map(c => {
          return new Promise((res,rej) =>  {
            c.onTransmit = (inReceiver,data) => {
              try {
                expect(inReceiver).to.be.equal(receiver);
                expect(data).to.be.deep.equal(test.data);
                res();
              }
              catch(e) {rej(e);}
            };
          });
        }));

        const prepPackage = Transport.prepareMultiTransmit(receiver,test.data,
          {processComplexTypes: test.processComplexTypes});
        [comA1,comA2].forEach(c => c.sendPackage(prepPackage))

        await receivePromise;
      });
    })

  });

  describe('Invokes', () => {

    [
      {
        title: 'A should receive the response of invoke with JSON data.',
        respData: {name: 'Luca', age: 21},
        expectedData: {name: 'Luca', age: 21},
        processComplexTypes: false
      },
      {
        title: 'A should receive the response of invoke with quotes in the procedure name.',
        procedure: 'Send"Message',
        respData: "Hello World!",
        expectedData: "Hello World!",
        processComplexTypes: false
      },
      {
        title: 'A should receive the response of invoke with binary data.',
        respData: new ArrayBuffer(200),
        expectedData: new ArrayBuffer(200),
        processComplexTypes: true
      },
      {
        title: 'A should receive the response of invoke with MixedJSON and internal special keys (_b and _s).',
        respData: {_b: new ArrayBuffer(5),cover: new ArrayBuffer(15),_s: 10,test: {_b: 10}},
        expectedData: {_b: new ArrayBuffer(5),cover: new ArrayBuffer(15),_s: 10,test: {_b: 10}},
        processComplexTypes: true
      },
      {
        title: 'A should receive the response of invoke but without binary data (processComplexTypes: false).',
        respData: new ArrayBuffer(200),
        expectedData: {},
        processComplexTypes: false
      },
      {
        title: 'A should receive the response of invoke but without read stream (processComplexTypes: false).',
        respData: new WriteStream(),
        expectedData: '[WriteStream]',
        processComplexTypes: false
      }
    ].forEach(test => {
      it(test.title, async () => {
        const procedure = test.procedure || 'someProcedure';
        comB1.onInvoke = (inProcedure,_,end) => {
          expect(inProcedure).to.be.equal(procedure);
          end(test.respData,test.processComplexTypes);
        };
        return comA1.invoke(procedure).then(result => {
          expect(result).to.be.deep.equal(test.expectedData);
        });
      });
    })

    it('A should receive the same invoked binary data as a response.', (done) => {
      const binary = new ArrayBuffer(200);
      comB1.onInvoke = (procedure,data,end) => {
        expect(procedure).to.be.equal('getSameBinary');
        end(data,true);
      };
      comA1.invoke('getSameBinary',binary,{processComplexTypes: true}).then(result => {
        expect(result).to.be.deep.equal(binary);
        done();
      });
    });

    it('B should receive the invoke with MixedJSON data.', (done) => {

      let writtenCode: any[];
      const writeStream = new WriteStream();
      (async () => {
        const chunk1 = new ArrayBuffer(20);
        const chunk2 = new ArrayBuffer(25);
        writtenCode = [chunk1,chunk2];
        await writeStream.write(chunk1,true);
        await writeStream.end(chunk2,true);
      })()

      const car = {avatar: new ArrayBuffer(20), model: 'X1', hp: 500, code: writeStream};
      comB1.onInvoke = async (procedure,data,end) => {
        expect(procedure).to.be.equal('car');

        expect(data.avatar).to.be.deep.equal(car.avatar);
        expect(data.model).to.be.equal(car.model);
        expect(data.hp).to.be.equal(car.hp);
        expect(data.code).to.be.instanceOf(ReadStream);

        const codeReadStream: ReadStream = data.code;
        codeReadStream.accept();
        const chunks: any[] = await codeReadStream.readAll();

        expect(chunks).to.be.deep.equal(writtenCode);
        end(1);
      };
      comA1.invoke('car',car,{processComplexTypes: true}).then(result => {
        expect(result).to.be.equal(1);
        done();
      });
    });

    it('A should receive the response with MixedJSON data of an invoke.', (done) => {
      let tv;
      let writtenCode: any[];
      comB1.onInvoke = (procedure,_,end) => {
        expect(procedure).to.be.equal('tv');

        const writeStream = new WriteStream();
        (async () => {
          const chunk1 = new ArrayBuffer(20);
          const chunk2 = new ArrayBuffer(25);
          writtenCode = [chunk1,chunk2];
          writeStream.write(chunk1,true);
          writeStream.end(chunk2,true);
        })();
        tv = {avatar: new ArrayBuffer(20), model: 'Y1', code: writeStream};
        end(tv,true);
      };
      comA1.invoke('tv').then(async result => {
        expect(result.avatar).to.be.deep.equal(tv.avatar);
        expect(result.model).to.be.equal(result.model);

        const codeReadStream: ReadStream = result.code;
        codeReadStream.accept();
        const chunks: any[] = await codeReadStream.readAll();
        expect(chunks).to.be.deep.equal(writtenCode);
        done();
      });
    });

    it('A should receive the err response of an invoke.', (done) => {
      const error = new Error('Some msg');
      error.name = 'SomeName';
      comB1.onInvoke = (procedure,_data,_end,reject) => {
        expect(procedure).to.be.equal('getErr');
        reject(error);
      };
      comA1.invoke('getErr').catch(err => {
        expect(err.name).to.be.equal(error.name);
        expect(err.message).to.be.equal(error.message);
        done();
      });
    });

    it('A should receive a timeout error by an unknown procedure invoke.', (done) => {
      comB1.onInvoke = () => {};
      comA1.invoke('?',undefined,{responseTimeout: 50}).catch(err => {
        expect(err).to.be.instanceof(TimeoutError)
        done();
      });
    });

    it('A should receive a connection lost error by an invoke with connection lost.', (done) => {
      comB1.onInvoke = () => {};
      comA1.invoke('?').catch(err => {
        expect(err).to.be.instanceof(BadConnectionError)
        done();
        comA1.emitConnection();
      });
      comA1.emitBadConnection(BadConnectionType.Disconnect);
    });

    it('A should receive the invoke response with the data type.', (done) => {
      comB1.onInvoke = (procedure,_data,end) => {
        expect(procedure).to.be.equal('procedure2');
        end({name: 'Leo'});
      };
      comA1.invoke('procedure2',undefined,{returnDataType: true}).then(res => {
        expect(res).to.be.deep.equal([{name: 'Leo'},DataType.JSON]);
        done();
      });
    });

  });

  describe('LinkedBuffer', () => {
    [
      [
        {size: 10,data: {}},
        {size: 5,data: {}},
        {size: 24,data: {}},
        {size: 60,data: {}},
        {size: 30,data: {}}
      ],
      [
        {size: 900,data: {}},
      ]
    ].forEach((testData,i) => {
      it(`LinkedBuffer should successfully store data - ${i}.`,() => {
        const buffer = new LinkedBuffer();
        let size = 0;
        for(let i = 0; i < testData.length; i++) {
          const data = testData[i];
          size += data.size;
          buffer.push(data,data.size);
        }

        expect(buffer.length).to.be.equal(testData.length);
        expect(buffer.size).to.be.equal(size);

        let leftSize = size;
        for(let i = 0; i < testData.length; i++) {
          const data = testData[i];
          leftSize -= data.size;

          const item = buffer.shift();
          expect(item).to.be.equal(data);
          expect(buffer.size).to.be.equal(leftSize);
        }
        expect(leftSize).to.be.equal(0);
      });
    })
  });

  describe('Streams', () => {
    [
      {
        binary: false,
        bufferSize: 200,
        data: generateArray(100,i => i),
        processComplexTypes: true,
      },
      {
        binary: false,
        bufferSize: 1,
        data: generateArray(137,i => i),
        processComplexTypes: false
      },
      {
        binary: false,
        bufferSize: 1,
        data: generateArray(23,i => new ArrayBuffer(i)),
        processComplexTypes: true
      },
      {
        binary: false,
        bufferSize: 8,
        data: generateArray(32,i =>
            ({name: `Name: ${i}`,age: i,image: new ArrayBuffer(16)})),
        processComplexTypes: true
      },
      {
        binary: false,
        bufferSize: 50,
        data: generateArray(100,i => i),
        simulateSlowRead: true,
        readBreakInterval: 20
      },
      {
        binary: false,
        bufferSize: 1,
        data: generateArray(100,i => i),
        simulateSlowRead: true,
        readBreakInterval: 3
      },
      {
        binary: true,
        bufferSize: 8192,
        data: generateArray(137,i => new ArrayBuffer(i)),
      },
      {
        binary: true,
        bufferSize: 1,
        data: generateArray(29,i => new ArrayBuffer(i)),
      },
      {
        binary: true,
        bufferSize: 8192,
        data: generateArray(224,i =>
            (new Uint8Array(generateArray(randomIntFromInterval(15,30),
                () => randomIntFromInterval(0,100)))).buffer),
      },
      {
        binary: true,
        bufferSize: 1,
        data: generateArray(13,i =>
            (new Uint8Array(generateArray(randomIntFromInterval(15,30),
                () => randomIntFromInterval(0,100)))).buffer),
      },
      {
        binary: true,
        bufferSize: 8192,
        data: generateArray(137,i => new ArrayBuffer(i)),
        simulateSlowRead: true,
        readBreakInterval: 20
      },
      {
        binary: true,
        bufferSize: 1,
        data: generateArray(28,i => new ArrayBuffer(i)),
        simulateSlowRead: true,
        readBreakInterval: 50
      },
    ].forEach((test,index) => {
      it(`B should receive the data of an ${test.binary ?
          'binary' : 'object'} stream fully${test.simulateSlowRead ? " (SLOW READ)" : ""
      } - ${index}.`,function (done)
      {
        this.timeout(10000);

        const writeStream = new WriteStream(test.binary);
        (async () => {
          for(let i = 0; i < (test.data.length - 1); i++) {
            if(!test.binary) await (writeStream as WriteStream<false>).write(test.data[i],test.processComplexTypes);
            else await (writeStream as WriteStream<true>).write(test.data[i]);
          }
          if(!test.binary) await (writeStream as WriteStream<false>)
              .end(test.data[test.data.length - 1],test.processComplexTypes);
          else await (writeStream as WriteStream<true>)
              .end(test.data[test.data.length - 1]);
        })();

        comB1.onTransmit = async (receiver, data: ReadStream, type) => {
          expect(receiver).to.be.equal('streamJson');
          expect(type).to.be.equal(DataType.Stream);
          expect(data).to.be.instanceof(ReadStream);

          test.readBreakInterval = test.readBreakInterval || 5;
          data.accept({bufferSize: test.bufferSize});
          let chunks: any[] = [];
          let chunk = await data.read();
          let i = 0;
          while (chunk != null) {
            chunks.push(chunk);
            if(test.simulateSlowRead && i % test.readBreakInterval === 0) {
              await new Promise(r => setTimeout(r,80))
            }
            chunk = await data.read();
            i++;
          }
          expect(data.closeCode).to.be.equal(StreamCloseCode.End);
          expect(data.closedSuccessfully).to.be.equal(true);
          expect(test.binary ? concatenateBuffer(...chunks) : chunks).to.be.deep
              .equal(test.binary ? concatenateBuffer(...test.data) : test.data);
          done();
        };
        comA1.transmit('streamJson',writeStream,{processComplexTypes: true});
      })
    });

    it("A's write stream should be closed when B closes the read stream.", (done) => {
      const writeStream = new WriteStream();
      writeStream.closed.then((code) => {
        expect(code).to.be.equal(505);
        done();
      });
      comB1.onTransmit = (receiver, data: ReadStream, type) => {
        expect(receiver).to.be.equal('readStreamClose');
        expect(type).to.be.equal(DataType.Stream);
        expect(data).to.be.instanceof(ReadStream);
        data.close(505)
      };
      comA1.transmit('readStreamClose',writeStream,{processComplexTypes: true});
    });

    it("The writer should write correctly.", (done) => {
      const writeStream = new WriteStream();
      const result: number[] = [];
      let i = 1;
      writeStream.useWriter((write, end) => {
        if(i > 10) return end();
        result.push(i);
        write(i++)
      });

      comB1.onTransmit = async (receiver, data: ReadStream, type) => {
        expect(receiver).to.be.equal('readStreamClose');
        expect(type).to.be.equal(DataType.Stream);
        expect(data).to.be.instanceof(ReadStream);
        data.accept();
        expect(await data.readAll()).to.be.deep.equal(result);
        done();
      };
      comA1.transmit('readStreamClose',writeStream,{processComplexTypes: true});
    });

    it("The chunk middleware should be able to update chunks.", (done) => {
      const writeStream = new WriteStream(true);
      let i = 1;
      writeStream.useWriter((write, end) => {
        if(i > 3) return end();
        write(new ArrayBuffer(i++))
      });

      comB1.onTransmit = async (receiver, data: ReadStream, type) => {
        expect(receiver).to.be.equal('chunkMiddleware');
        expect(type).to.be.equal(DataType.Stream);
        expect(data).to.be.instanceof(ReadStream);

        const result: ArrayBuffer[] = [];
        data.chunkMiddleware = (chunk, updateChunk) => {
          const newBuffer = (new Uint8Array(generateArray(randomIntFromInterval(20,30),
              () => randomIntFromInterval(0,100)))).buffer;
          updateChunk(newBuffer);
          result.push(newBuffer);
          return true;
        }
        data.accept({sizeLimit: 8});
        expect(concatenateBuffer(...await data.readAll()))
            .to.be.deep.equal(concatenateBuffer(...result));
        done();
      };
      comA1.transmit('chunkMiddleware',writeStream,{processComplexTypes: true});
    });

    it("Ignored read stream should end write stream with an accept timeout.", (done) => {
      const writeStream = new WriteStream(false,{acceptTimeout: 60});
      writeStream.closed.then((code) => {
        expect(code).to.be.equal(StreamCloseCode.AcceptTimeout);
        done();
      });
      comB1.onTransmit = (receiver,data: ReadStream) => {
        expect(receiver).to.be.equal('streamAcceptTimeout');
      };
      comA1.transmit('streamAcceptTimeout',writeStream,{processComplexTypes: true});
    });

    it("An accept read stream which will not receive anything a certain time should end with chunk timeout.", (done) => {
      const writeStream = new WriteStream();

      comB1.onTransmit = (receiver,data: ReadStream) => {
        expect(receiver).to.be.equal('streamChunkTimeout');

        data.closed.then((code) => {
          expect(code).to.be.equal(StreamCloseCode.ChunkTimeout);
          done();
        });
        data.accept({
          bufferSize: 6,
          chunkTimeout: 60
        });

      };
      comA1.transmit('streamChunkTimeout',writeStream,{processComplexTypes: true});
    });

    it("An accepted read stream with long time full buffer should trigger the write stream size permission timeout.", (done) => {
      const writeStream = new WriteStream(false,{
        sizePermissionTimeout: 50
      });
      writeStream.closed.then((code) => {
        expect(code).to.be.equal(StreamCloseCode.SizePermissionTimeout);
        done();
      });

      comB1.onTransmit = (receiver,data: ReadStream) => {
        expect(receiver).to.be.equal('sizePermissionTimeout');
        data.accept({
          bufferSize: 1,
        });
      };
      comA1.transmit('sizePermissionTimeout',writeStream,{processComplexTypes: true});

      let i = 0;
      writeStream.useWriter((write) =>  {
        write(i > 50 ? null : i++);
      });
    });

    it("Write stream that doesn't get any close package after send EOF should trigger endClosureTimeout.", (done) => {
      const writeStream = new WriteStream(false,{
        endClosureTimeout: 50
      });
      writeStream.closed.then((code) => {
        expect(code).to.be.equal(StreamCloseCode.EndClosureTimeout);
        done();
      });

      comB1.onTransmit = (receiver,data: ReadStream) => {
        expect(receiver).to.be.equal('endClosureTimeout');
        data.chunkMiddleware = async () => {
          //blocking processing
          await new Promise(r => setTimeout(r,200));
          return true;
        }
        data.accept();
      };
      comA1.transmit('endClosureTimeout',writeStream,{processComplexTypes: true});

      (async () => {
        await writeStream.write(1);
        await writeStream.end();
      })()
    });

    it("WriteStream should reject write and end calls when end was already called.", async () => {
      const writeStreamObj = new WriteStream(false);
      const writeStreamBin = new WriteStream(true);

      comB1.onTransmit = (receiver,data: ReadStream[]) => {
        expect(receiver).to.be.equal('writeStreams');
        data.forEach(stream => stream.accept());
      };
      comA1.transmit('writeStreams',[writeStreamObj,writeStreamBin],
          {processComplexTypes: true});

      await Promise.all([writeStreamObj,writeStreamBin].map(async (stream) => {
        await stream.end(new ArrayBuffer(10));
        await Promise.all([
            expect(stream.end()).to.be.rejectedWith(Error),
            expect(stream.write(new ArrayBuffer(10))).to.be.rejectedWith(Error)
        ]);
      }));
    });

    it("WriteStream should wait for low backpressure before sending next chunk.", async () => {
      const streams = [new WriteStream(false),new WriteStream(true)];

      let dataCounter = 0;

      comB1.onTransmit = (receiver,data: ReadStream[]) => {
        expect(receiver).to.be.equal('writeStreams');
        data.forEach(stream => {
          stream.accept();
          (async () => {
            while(await stream.read() !== null) dataCounter++;
          })();
        });
      };
      comA1.transmit('writeStreams',streams, {processComplexTypes: true});

      let backpressure = true;
      comA1.hasLowSendBackpressure = () => !backpressure;

      for(const stream of streams) {
        stream.write(new ArrayBuffer(10));
        await new Promise(r => setTimeout(r,50));

        //Should not receive the chunk.. after waiting.
        expect(dataCounter).to.be.equal(0);

        //drain
        backpressure = false;
        comA1.emitSendBackpressureDrain();
        await new Promise(r => setTimeout(r,20));

        //Should receive the chunk.. after drain.
        expect(dataCounter).to.be.equal(1);

        //reset
        dataCounter = 0;
        backpressure = true;
      }
    });

    it('Transmit a stream and a connection lost on B after sending should close the ReadStream after accepting.', (done) => {

      let simulatedBadConnectionPromise;

      comB1.onTransmit = async (receiver,data: ReadStream) => {
        await simulatedBadConnectionPromise;
        expect(receiver).to.be.equal('stream');
        expect(data).to.be.instanceOf(ReadStream);
        expect(data.state).to.be.equal(StreamState.Pending);

        data.closed.then((code) => {
          expect(code).to.be.equal(StreamCloseCode.BadConnection);
          done();
          comB1.emitConnection();
        });
        data.accept();
      };

      simulatedBadConnectionPromise = new Promise(async (res) => {
        await comA1.transmit('stream',new WriteStream(),{processComplexTypes: true});
        comB1.emitBadConnection(BadConnectionType.Disconnect);
        res();
      })
    });

    [{
      binary: true
    },{
      binary: false
    }].forEach(test => {
      it(`SizeLimit option should work properly in ${test.binary ? 'binary' : 'object'} mode.`, (done) => {
        const writeStream = new WriteStream(test.binary);

        let i = 1;
        writeStream.useWriter(write => {
          write(i < 10 ?
              (test.binary ? new ArrayBuffer(i++) : i++) as any : null);
        })

        writeStream.closed.then((code) => {
          expect(code).to.be.equal(StreamCloseCode.SizeLimitExceeded);
          done();
        });

        comB1.onTransmit = (receiver,data: ReadStream) => {
          expect(receiver).to.be.equal('streamSizeLimit');
          data.accept({sizeLimit: 2,bufferSize: 50});
        };
        comA1.transmit('streamSizeLimit',writeStream,{processComplexTypes: true});
      });
    })

    it('Transmit a stream and a connection lost on A after sending should close the WriteStream.', async () => {
      const writeStream = new WriteStream();
      await comA1.transmit('stream',writeStream,{processComplexTypes: true});
      comA1.emitBadConnection(BadConnectionType.Disconnect);

      expect(writeStream.state).to.be.equal(StreamState.Closed);
    });

    it('ReadAll method of a failure closed ReadStream should throw a StreamCloseError.', async () => {
      let readStream: ReadStream | null = null;
      comB1.onTransmit = async (receiver,data: ReadStream) => {
        readStream = data;
      };
      await comA1.transmit('stream',new WriteStream(),{processComplexTypes: true});

      expect(readStream).to.be.instanceOf(ReadStream);
      expect(readStream!.state).to.be.equal(StreamState.Pending);
      readStream!.close(StreamCloseCode.Abort);
      await expect(readStream!.readAll()).to.be.rejectedWith(StreamCloseError);
    });
  });

  describe('Ping/Pong', () => {

    it('B should receive ping.', (done) => {
      comB1.onPing = () => done();
      comA1.sendPing();
    });

    it('B should receive pong.', (done) => {
      comB1.onPong = () => done();
      comA1.sendPong();
    });

  });

  describe('Analyse typeof data', () => {

    [
      {
        data: undefined,
        expectedType: DataType.JSON
      },
      {
        data: null,
        expectedType: DataType.JSON
      },
      {
        data: 10,
        expectedType: DataType.JSON
      },
      {
        data: '',
        expectedType: DataType.JSON
      },
      {
        data: {},
        expectedType: DataType.JSON
      },
      {
        data: [],
        expectedType: DataType.JSON
      },
      {
        data: {persons: [{name: 'Peter'},{name: 'Mauri'}]},
        expectedType: DataType.JSON
      },
      {
        data: new ArrayBuffer(10),
        expectedType: DataType.Binary
      },
      {
        data: new WriteStream(),
        expectedType: DataType.Stream
      },
      {
        data: {images: [new ArrayBuffer(10)]},
        expectedType: DataType.JSONWithBinaries
      },
      {
        data: {images: [new WriteStream()]},
        expectedType: DataType.JSONWithStreams
      },
      {
        data: {images: [new WriteStream()],car: {color: 'black', code: new ArrayBuffer(10)}},
        expectedType: DataType.JSONWithStreamsAndBinaries
      },
      {
        data: {images: [new WriteStream(),new ArrayBuffer(10),'']},
        expectedType: DataType.JSONWithStreamsAndBinaries
      }
    ].forEach((test,index) => {
      it(`Analyse typeof data test: ${index}`, () => {
        expect(analyseTypeofData(test.data)).to.be.equal(test.expectedType);
      });
    })

  });

  describe('Batching', () => {
    it('All text batch transmits should be received.', (done) => {
      const count = 43;

      let receivedI = 0;
      comB1.onTransmit = (receiver,data) => {
        expect(receiver).to.be.equal('batch');
        expect(data).to.be.equal('msg');
        receivedI++;
        if(receivedI === count) done();
      };

      comA1.options.maxBufferChunkLength = 5;
      comA1.options.limitBatchStringLength = 2;
      for(let i = 0; i < count; i++){
        comA1.transmit('batch','msg',{batch: 50});
      }
    });

    it('All binary batch transmits should be received.', (done) => {
      const count = 83;

      const sentData: any[] = [];
      const receivedData: any[] = [];
      comB1.onTransmit = (receiver,data) => {
        expect(receiver).to.be.equal('batch');
        receivedData.push(data);
        if(receivedData.length === count) {
          expect(receivedData).to.be.deep.equal(sentData);
          done();
        }
      };

      comA1.options.maxBufferChunkLength = 5;
      comA1.options.limitBatchBinarySize = 40;
      for(let i = 0; i < count; i++){
        const data = new ArrayBuffer(i);
        sentData.push(data);
        comA1.transmit('batch',data,{processComplexTypes: true,batch: 50});
      }
    });

    it('Batch package order should not be messed up.', (done) => {
      const sendMessages: any[] = [];
      const receivedMessages: any[] = [];
      const dataLength = 211;

      let receivedI = 0;
      comB1.onTransmit = (receiver,data) => {
        expect(receiver).to.be.equal('batch');
        receivedI++;
        receivedMessages.push(data);
        if(receivedI === dataLength) {
          expect(receivedMessages).to.be.deep.equal(sendMessages);
          done();
        }
      };

      comA1.options.maxBufferChunkLength = 20;
      comA1.options.limitBatchStringLength = 20000;
      for(let i = 0; i < dataLength; i++){
        //send text and binary packages
        const data = i % 4 === 0 ? new ArrayBuffer(i) : i % 10 === 0 ? {
          b: new ArrayBuffer(20),
          c: {b2: new ArrayBuffer(i)}
        } : i;
        sendMessages.push(data);
        comA1.transmit('batch',data,{batch: 100,
          processComplexTypes: i % 4 === 0 || i % 10 === 0});
      }
    });

    describe('Batch 0 (one bundle per event loop turn)', () => {
      const createBuffer = () => {
        const sends: (ArrayBuffer | string)[][] = [];
        const buffer = new PackageBuffer((messages) => sends.push(messages));
        return {buffer, sends};
      };

      it('Should count 0 as batched, but not null or undefined.', () => {
        expect(isBatched(0)).to.be.true;
        expect(isBatched(50)).to.be.true;
        expect(isBatched(true)).to.be.true;
        expect(isBatched(null)).to.be.false;
        expect(isBatched(undefined)).to.be.false;
      });

      it('Should send N packages of one turn as exactly one message at the end of the turn.', (done) => {
        const {buffer, sends} = createBuffer();
        const count = 25;
        for(let i = 0; i < count; i++) buffer.add(Transport.prepareMultiTransmit('turn',i),0);
        expect(sends.length).to.be.equal(0);
        setImmediate(() => {
          expect(sends.length).to.be.equal(1);
          expect(sends[0].length).to.be.equal(1);
          const bundle = sends[0][0] as string;
          expect(JSON.parse(bundle.substring(bundle.indexOf(',') + 1)).length).to.be.equal(count);
          expect(buffer.getBufferLength()).to.be.equal(0);
          done();
        });
      });

      it('Should send a single package at the end of its turn without waiting for a timer.', (done) => {
        const {buffer, sends} = createBuffer();
        const start = Date.now();
        buffer.add(Transport.prepareMultiTransmit('turn','one'),0);
        setImmediate(() => {
          expect(sends.length).to.be.equal(1);
          expect(Date.now() - start).to.be.lessThan(20);
          done();
        });
      });

      it('Should flush the buffers of one turn together, in the order of their first package.', (done) => {
        const order: string[] = [];
        const buffers = ['a', 'b', 'c'].map(name => new PackageBuffer(() => order.push(name)));
        buffers[1].add(Transport.prepareMultiTransmit('turn',1),0);
        buffers[0].add(Transport.prepareMultiTransmit('turn',2),0);
        buffers[1].add(Transport.prepareMultiTransmit('turn',3),0);
        buffers[2].add(Transport.prepareMultiTransmit('turn',4),0);
        //flushed by hand before the end of the turn: not again
        buffers[2].flushBuffer();
        expect(order).to.be.deep.equal(['c']);
        setImmediate(() => {
          expect(order).to.be.deep.equal(['c', 'b', 'a']);
          buffers[0].add(Transport.prepareMultiTransmit('turn',5),0);
          setImmediate(() => {
            expect(order).to.be.deep.equal(['c', 'b', 'a', 'a']);
            done();
          });
        });
      });

      it('Should flush the other buffers of the turn when the send of one throws.', (done) => {
        const sent: string[] = [];
        const failing = new PackageBuffer(() => {throw new Error('send failed');});
        const fine = new PackageBuffer(() => sent.push('fine'));
        failing.add(Transport.prepareMultiTransmit('turn',1),0);
        fine.add(Transport.prepareMultiTransmit('turn',2),0);
        const listeners = process.listeners('uncaughtException');
        process.removeAllListeners('uncaughtException');
        process.once('uncaughtException', (err) => {
          for(const listener of listeners) process.on('uncaughtException',listener);
          expect(err.message).to.be.equal('send failed');
          expect(sent).to.be.deep.equal(['fine']);
          done();
        });
      });

      it('Should bundle each turn on its own.', (done) => {
        const {buffer, sends} = createBuffer();
        buffer.add(Transport.prepareMultiTransmit('turn',1),0);
        buffer.add(Transport.prepareMultiTransmit('turn',2),0);
        setImmediate(() => {
          buffer.add(Transport.prepareMultiTransmit('turn',3),0);
          setImmediate(() => {
            expect(sends.length).to.be.equal(2);
            done();
          });
        });
      });

      it('Should flush packages with a pending timer at the end of the turn as well.', (done) => {
        const {buffer, sends} = createBuffer();
        buffer.add(Transport.prepareMultiTransmit('turn','late'),100);
        buffer.add(Transport.prepareMultiTransmit('turn','now'),0);
        buffer.add(Transport.prepareMultiTransmit('turn','late again'),100);
        setImmediate(() => {
          expect(sends.length).to.be.equal(1);
          expect(buffer.getBufferLength()).to.be.equal(0);
          setTimeout(() => {
            expect(sends.length).to.be.equal(1);
            done();
          },130);
        });
      });

      it('Should not send when the only package was removed in the same turn.', (done) => {
        const {buffer, sends} = createBuffer();
        const pack = Transport.prepareMultiTransmit('turn','gone');
        buffer.add(pack,0);
        expect(buffer.tryRemove(pack)).to.be.true;
        setImmediate(() => {
          expect(sends.length).to.be.equal(0);
          done();
        });
      });

      it('Should keep the packages while the source is closed and send them on a flush.', (done) => {
        let open = false;
        const sends: (ArrayBuffer | string)[][] = [];
        const buffer = new PackageBuffer((messages) => sends.push(messages),() => open);
        buffer.add(Transport.prepareMultiTransmit('turn',1),0);
        setImmediate(() => {
          expect(sends.length).to.be.equal(0);
          expect(buffer.getBufferLength()).to.be.equal(1);
          open = true;
          expect(buffer.flushBuffer()).to.be.true;
          expect(sends.length).to.be.equal(1);
          done();
        });
      });

      it('Should receive all transmits with batch 0 in order.', (done) => {
        const count = 40;
        const received: any[] = [];
        comB1.onTransmit = (receiver,data) => {
          expect(receiver).to.be.equal('turn');
          received.push(data);
          if(received.length === count) {
            expect(received).to.be.deep.equal(generateArray(count,(i) => i));
            done();
          }
        };
        comA1.options.maxBufferChunkLength = 200;
        comA1.options.limitBatchStringLength = 310000;
        for(let i = 0; i < count; i++) comA1.transmit('turn',i,{batch: 0});
      });

      it('Should send group transmits with batch 0 of one turn as one message per group.', (done) => {
        const sends: Record<string,number> = {};
        const group = new DynamicGroupTransport({
          send: (group) => {sends[group] = (sends[group] || 0) + 1;},
          isConnected: () => true
        });
        for(let i = 0; i < 10; i++) {
          group.transmit('A','turn',i,{batch: 0});
          group.transmit('B','turn',i,{batch: 0});
        }
        expect(sends).to.be.deep.equal({});
        setImmediate(() => {
          expect(sends).to.be.deep.equal({A: 1, B: 1});
          done();
        });
      });
    });

    it('Should not send cancelled batch packages.', (done) => {
      const count = 10;

      let receivedI = 0;
      comB1.onTransmit = () => {
        receivedI++;
      };

      const packages: Package[] = [];
      for(let i = 0; i < count; i++){
        packages.push(Transport.prepareMultiTransmit('batch','msg'));
        comA1.sendPackage(packages[i],10);
      }
      packages.forEach(pack => comA1.tryCancelPackage(pack));

      setTimeout(() => {
        expect(receivedI).to.be.equal(0);
        done();
      },50)
    });
  });

  describe('Stability', () => {

    [
      {
        msg: '[]',
        invalid: true
      },
      {
        msg: '10',
        invalid: true
      },
      {
        msg: '{name:Tim}',
        invalid: true
      },
      {
        msg: '1,"someReceiver",342',
        invalid: true
      },
      {
        msg: '1,"someReceiver",0',
        invalid: false
      },
      {
        msg: '1,10,0',
        invalid: true
      },
      {
        msg: '1,{},0',
        invalid: true
      },
      {
        msg: '1,"someReceiver",1',
        invalid: true
      },
      {
        msg: '1,"someReceiver",2',
        invalid: true
      },
      {
        msg: new ArrayBuffer(0),
        invalid: true
      },
      {
        msg: new ArrayBuffer(10),
        invalid: true
      },
    ].forEach((test, index) => {

      it(`Transmit stability invalid msg test: ${index}`, () => {
        return new Promise((res,rej) => {
          comB1.onInvalidMessage = (e) => {
            try{
              expect(test.invalid).to.be.true;
              res();
            }
            catch(err) {rej(err)}
          }
          comB1.onTransmit = () => {
            try{
              expect(test.invalid).to.be.false;
              res();
            }
            catch(err) {rej(err)}
          }
          comA1.send(test.msg);
        })
      });

    });

    [
      {
        msg: '[]',
        invalid: true
      },
      {
        msg: '10',
        invalid: true
      },
      {
        msg: '2,"someProcedure",0,0',
        invalid: false
      },
      {
        msg: '2,"someProcedure","callIdWrong",0',
        invalid: true
      },
    ].forEach((test, index) => {

      it(`Invoke stability invalid msg test: ${index}`, () => {
        return new Promise((res,rej) => {
          comB1.onInvalidMessage = (e) => {
            try{
              expect(test.invalid).to.be.true;
              res();
            }
            catch(err) {rej(err)}
          }
          comB1.onInvoke = (_procedure,_data,end) => {
            try{
              expect(test.invalid).to.be.false;
              end();
              res();
            }
            catch(err) {rej(err)}
          }
          comA1.send(test.msg);
        })
      });

    });

    it(`Throwing into a listener should call listener error event.`, () => {
      return new Promise((res,rej) => {
        comB1.onListenerError = (err) => {
          try {
            expect(err).to.be.instanceOf(Error);
            expect(err.name).to.be.equal('SomeName');
            expect(err.message).to.be.equal('SomeMsg');
            res();
          }
          catch(err) {rej(err)}
        }
        comB1.onTransmit = () => {
          const err = new Error('SomeMsg');
          err.name = 'SomeName';
          throw err;
        }
        comA1.transmit('receiver');
      })
    });

    it(`Using the same write stream multiple times should throw an error.`, () => {
      const stream = new WriteStream();
      comA1.transmit('receiver',stream,{processComplexTypes: true});

      expect(function () {
        comA1.transmit('receiver',stream,{processComplexTypes: true})
      }).to.throw(Error,'Write-stream already used.');
    });

    it(`Should throw InsufficientBufferSizeError when transmitting a package that does not fit in the buffer (With an unconnected source).`, () => {
      comA1.emitBadConnection(BadConnectionType.Disconnect);
      comA1.options.maxBufferSize = 200;
      expect(function () {
        comA1.transmit("test",{pic: new ArrayBuffer(400)},{processComplexTypes: true});
      }).to.throw(InsufficientBufferSizeError);
    });

    it(`Mixed invoke and transmit messages order should be stable.`, (end) => {
      const count = 500;
      const sent: number[] = [];
      const received: number[] = [];

      const handler = (target: string,msg: any) => {
        expect(target).to.be.equal("order");
        received.push(msg);

        if(received.length === count) {
          expect(received).to.be.deep.equal(sent);
          end();
        }
      }
      comB1.onInvoke = handler;
      comB1.onTransmit = handler;

      for(let i = 0; i < count; i++) {
        sent.push(i);
        if(i % 2 === 0) comA1.transmit('order',i);
        else comA1.invoke('order',i);
      }
    });
  });

  describe('GroupTransport (Utility)', () => {

    [
      {
        data: 'msg',
        batch: 50
      },
      {
        data: {pic: new ArrayBuffer(10),code: new ArrayBuffer(30)},
        processComplexTypes: true
      },
      {
        data: new ArrayBuffer(10),
        processComplexTypes: true,
        batch: 20
      }
    ].forEach((test, index) => {

      it(`Should correctly send multiple transmits to all underlying transporters: ${index}`, (done) => {
        comBGroup.options.limitBatchStringLength = 50;

        const count = 20;

        let receivedI = 0;
        const receiver = (receiver,data) => {
          expect(receiver).to.be.equal('group');
          expect(data).to.be.deep.equal(test.data);
          receivedI++;
          if(receivedI === (count * 2)) done();
        }
        comB1.onTransmit = receiver;
        comB2.onTransmit = receiver;

        for(let i = 0; i < count; i++)
          comBGroup.transmit('group',test.data,{
            batch: test.batch,
            processComplexTypes: test.processComplexTypes
          });
      });
    });

  });

  describe('DynamicGroupTransport (Utility)', () => {

    [
      {
        data: 'msg',
        batch: 50,
        limitBatchStringLength: Number.POSITIVE_INFINITY
      },
      {
        data: 'msg',
      },
      {
        data: {pic: new ArrayBuffer(10),code: new ArrayBuffer(30)},
        processComplexTypes: true
      },
      {
        data: new ArrayBuffer(10),
        processComplexTypes: true,
        batch: 20
      },
      {
        data: {name: 'Name',pic: new ArrayBuffer(20),code: new ArrayBuffer(5)},
        processComplexTypes: true,
        batch: 200
      }
    ].forEach((test, index) => {

      it(`Should correctly send multiple transmits to different groups: ${index}`, (done) => {
        dynamicGroup.options.limitBatchStringLength = test.limitBatchStringLength || 60;
        dynamicGroup.options.maxBufferChunkLength = Number.POSITIVE_INFINITY;
        dynamicGroup.options.freeBufferMaxPoolSize = 1;

        const aCount = 260;
        const bCount = 150;

        let received = 0;
        const createReceiver = (expectedReceiver: string) => {
          return (receiver,data) => {
            expect(receiver).to.be.equal(expectedReceiver);
            expect(data).to.be.deep.equal(test.data);
            received++;
            if(received === aCount * 2 + bCount * 2) done();
          }
        }
        const aReceiver = createReceiver("SendMessageA");
        const bReceiver = createReceiver("SendMessageB");
        comB1.onTransmit = bReceiver;
        comB2.onTransmit = bReceiver;
        comA1.onTransmit = aReceiver;
        comA2.onTransmit = aReceiver;

        const options = {
          batch: test.batch,
          processComplexTypes: test.processComplexTypes
        };

        for(let i = 0; i < aCount; i++)
          dynamicGroup.transmit("A",'SendMessageA',test.data,options);
        for(let i = 0; i < bCount; i++)
          dynamicGroup.transmit("B",'SendMessageB',test.data,options);
      });
    });
  });

});