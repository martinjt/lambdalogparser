using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;

namespace AWSLambdaLogParser
{
    public class MultiMemberGzipStream : Stream, IDisposable
    {
        private Stream _originalStream;
        private Stream _newStream = new MemoryStream();
        Stack<long> StartPositions = new Stack<long>();

        public override bool CanRead => _newStream.CanRead;

        public override bool CanSeek => _newStream.CanSeek;

        public override bool CanWrite => _newStream.CanWrite;

        public override long Length => _newStream.Length;

        public override long Position
        {
            get => _newStream.Position;
            set => _newStream.Position = value;
        }

        public MultiMemberGzipStream(Stream stream)
        {
            if (!stream.CanSeek)
                CreateMemoryStreamFromIncomingStream(stream);
            else
                _originalStream = stream;

            FindStartStreams();
            BuildNewStream();
        }

        public void CreateMemoryStreamFromIncomingStream(Stream incomingStream)
        {
            _originalStream = new MemoryStream();
            int count = 0;
            do
            {
                byte[] buf = new byte[1024];
                count = incomingStream.Read(buf, 0, 1024);
                _originalStream.Write(buf, 0, count);
            } while (incomingStream.CanRead && count > 0);
            _originalStream.Position = 0;
        }

        public void FindStartStreams()
        {
            while (_originalStream.Position < _originalStream.Length)
            {
                if (IsHeaderCandidate(_originalStream))
                {
                    StartPositions.Push(_originalStream.Position);
                }
                _originalStream.ReadByte();
            }
        }

        public void BuildNewStream()
        {
            while (StartPositions.Count > 0)
            {
                _originalStream.Seek(StartPositions.Pop(), SeekOrigin.Begin);
                using (var decompressedStream = new GZipStream(_originalStream, CompressionMode.Decompress, true))
                {
                    decompressedStream.CopyTo(_newStream);
                }
            }
            _newStream.Position = 0;
        }

        public override void Flush()
        {
            _newStream.Flush();
        }

        public override int Read(byte[] buffer, int offset, int count)
        {
            return _newStream.Read(buffer, offset, count);
        }

        public override long Seek(long offset, SeekOrigin origin)
        {
            return _newStream.Seek(offset, origin);
        }

        public override void SetLength(long value)
        {
            _newStream.SetLength(value);
        }

        public override void Write(byte[] buffer, int offset, int count)
        {
            _newStream.Write(buffer, offset, count);
        }

        const int Id1 = 0x1F;
        const int Id2 = 0x8B;
        const int DeflateCompression = 0x8;
        const int GzipFooterLength = 8;
        const int MaxGzipFlag = 32;

        /// <summary>
        /// Returns true if the stream could be a valid gzip header at the current position.
        /// </summary>
        /// <param name="stream">The stream to check.</param>
        /// <returns>Returns true if the stream could be a valid gzip header at the current position.</returns>
        public bool IsHeaderCandidate(Stream stream)
        {
            // Read the first ten bytes of the stream
            byte[] header = new byte[10];

            int bytesRead = stream.Read(header, 0, header.Length);
            stream.Seek(-bytesRead, SeekOrigin.Current);

            if (bytesRead < header.Length)
            {
                return false;
            }

            // Check the id tokens and compression algorithm
            if (header[0] != Id1 || header[1] != Id2 || header[2] != DeflateCompression)
            {
                return false;
            }

            // Extract the GZIP flags, of which only 5 are allowed (2 pow. 5 = 32)
            if (header[3] > MaxGzipFlag)
            {
                return false;
            }

            // Check the extra compression flags, which is either 2 or 4 with the Deflate algorithm
            if (header[8] != 0x0 && header[8] != 0x2 && header[8] != 0x4)
            {
                return false;
            }

            return true;
        }

        public new void Dispose()
        {
            _newStream.Dispose();
            _originalStream.Dispose();
        }
    }


}