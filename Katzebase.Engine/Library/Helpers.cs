﻿using Katzebase.PublicLibrary.Exceptions;
using System.Security.Cryptography;
using System.Text;

namespace Katzebase.Engine.Library
{
    public static class Helpers
    {
        public static void CopyDirectory(string sourcePath, string destinationPath)
        {
            //Now Create all of the directories
            foreach (string dirPath in Directory.GetDirectories(sourcePath, "*",
                SearchOption.AllDirectories))
                Directory.CreateDirectory(dirPath.Replace(sourcePath, destinationPath));

            //Copy all the files & Replaces any files with the same name
            foreach (string newPath in Directory.GetFiles(sourcePath, "*.*",
                SearchOption.AllDirectories))
                File.Copy(newPath, newPath.Replace(sourcePath, destinationPath), true);
        }

        public static string MakeSafeFileName(string filename)
        {
            Array.ForEach(Path.GetInvalidFileNameChars(),
                  c => filename = filename.Replace(c.ToString(), string.Empty));

            return filename;
        }

        public static ushort Checksum(string buffer)
        {
            return Checksum(Encoding.ASCII.GetBytes(buffer));
        }

        public static ushort Checksum(byte[] buffer)
        {
            ushort sum = 0;
            foreach (var b in buffer)
            {
                sum += (ushort)(sum ^ b);
            }
            return sum;
        }

        public static bool IsDirectoryEmpty(string path)
        {
            if (Directory.Exists(path))
            {
                return !Directory.EnumerateFileSystemEntries(path).Any();
            }
            return false;
        }

        public static void RemoveDirectoryIfEmpty(string? diskPath)
        {
            if (diskPath == null)
            {
                throw new KbNullException($"Value should not be null {nameof(diskPath)}.");
            }

            if (IsDirectoryEmpty(diskPath))
            {
                Directory.Delete(diskPath);
            }
        }

        public static string ComputeSHA256(string rawData)
        {
            byte[] bytes = SHA256.HashData(Encoding.UTF8.GetBytes(rawData));

            var builder = new StringBuilder();
            for (int i = 0; i < bytes.Length; i++)
            {
                builder.Append(bytes[i].ToString("x2"));
            }
            return builder.ToString();
        }
    }
}
