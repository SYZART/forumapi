/* eslint-disable max-len */
const pool = require('../../database/postgres/pool');
const UsersTableTestHelper = require('../../../../tests/UsersTableTestHelper');
const ThreadsTableTestHelper = require('../../../../tests/ThreadsTableTestHelper');
const container = require('../../container');
const createServer = require('../createServer');
const ServerTestHelper = require('../../../../tests/ServerTestHelper');
const AuthenticationsTableTestHelper = require('../../../../tests/AuthenticationsTableTestHelper');

describe('/threads endpoint', () => {
  afterAll(async () => {
    await pool.end();
  });

  afterEach(async () => {
    await UsersTableTestHelper.cleanTable();
    await ThreadsTableTestHelper.cleanTable();
    await AuthenticationsTableTestHelper.cleanTable();
  });

  describe('when POST /threads', () => {
    it('should response 401 when missing authentication', async () => {
      const reqPayload = {
        title: 'some thread',
        body: 'anything',
      };

      const server = await createServer(container);

      const response = await server.inject({
        method: 'POST',
        url: '/threads',
        payload: reqPayload,
      });

      const responseJson = JSON.parse(response.payload);
      expect(response.statusCode).toEqual(401);
      expect(responseJson.error).toEqual('Unauthorized');
    });

    it('should response 400 when request payload not contain needed property', async () => {
      const reqPayload = {
        title: 'some thread',
      };

      const server = await createServer(container);

      const { accessToken } = await ServerTestHelper
          .getAccessTokenAndUserIdHelper({ server });

      const response = await server.inject({
        method: 'POST',
        url: '/threads',
        payload: reqPayload,
        headers: {
          Authorization: `Bearer ${accessToken}`,
        },
      });

      const responseJson = JSON.parse(response.payload);
      expect(response.statusCode).toEqual(400);
      expect(responseJson.status).toEqual('fail');
      expect(responseJson.message)
          .toEqual('tidak dapat membuat thread baru karena properti yang dibutuhkan tidak ada');
    });

    it('should response 400 when request payload not meet data type spec', async () => {
      const reqPayload = {
        title: 'some thread',
        body: 123,
      };

      const server = await createServer(container);

      const { accessToken } = await ServerTestHelper
          .getAccessTokenAndUserIdHelper({ server });

      const response = await server.inject({
        method: 'POST',
        url: '/threads',
        payload: reqPayload,
        headers: {
          Authorization: `Bearer ${accessToken}`,
        },
      });

      const responseJson = JSON.parse(response.payload);
      expect(response.statusCode).toEqual(400);
      expect(responseJson.status).toEqual('fail');
      expect(responseJson.message)
          .toEqual('tidak dapat membuat thread baru karena tipe data tidak sesuai');
    });

    it('should response 201 and persisted thread', async () => {
      const reqPayload = {
        title: 'some thread',
        body: 'anything',
      };

      const server = await createServer(container);

      const { accessToken } = await ServerTestHelper
          .getAccessTokenAndUserIdHelper({ server });

      const response = await server.inject({
        method: 'POST',
        url: '/threads',
        payload: reqPayload,
        headers: {
          Authorization: `Bearer ${accessToken}`,
        },
      });

      const responseJson = JSON.parse(response.payload);
      expect(response.statusCode).toEqual(201);
      expect(responseJson.status).toEqual('success');
      expect(responseJson.data.addedThread).toBeDefined();
      expect(responseJson.data.addedThread.id).toBeDefined();
      expect(responseJson.data.addedThread.title).toEqual(reqPayload.title);
      expect(responseJson.data.addedThread.owner).toBeDefined();
    });
  });

  describe('when GET /threads/{id}', () => {
    it('should response 404 when thread is not found', async () => {
      const server = await createServer(container);

      const response = await server.inject({
        method: 'GET',
        url: '/threads/thread-123',
      });

      const responseJson = JSON.parse(response.payload);
      expect(response.statusCode).toEqual(404);
      expect(responseJson.status).toEqual('fail');
      expect(responseJson.message).toEqual('thread tidak ditemukan');
    });

    it('should response 200 and array of thread', async () => {
      await UsersTableTestHelper.addUser({});
      await ThreadsTableTestHelper.addThread({});

      const server = await createServer(container);

      const response = await server.inject({
        method: 'GET',
        url: '/threads/thread-123',
      });

      const responseJson = JSON.parse(response.payload);
      expect(response.statusCode).toEqual(200);
      expect(responseJson.data.thread).toBeDefined();
    });
  });
});
