// Copyright 2013 The Gorilla WebSocket Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package main

import "github.com/gorilla/websocket"

type IClient interface {
	WriteTextMessage([]byte) error
}

// hub maintains the set of active clients and broadcasts messages to the
// clients.
type Hub struct {
	// Registered clients.
	clients map[IClient]bool

	// Inbound messages from the clients.
	broadcast chan []byte

	// Register requests from the clients.
	register chan IClient

	// Unregister requests from clients.
	unregister chan IClient
}

func newHub() *Hub {
	return &Hub{
		broadcast:  make(chan []byte),
		register:   make(chan IClient),
		unregister: make(chan IClient),
		clients:    make(map[IClient]bool),
	}
}

func (h *Hub) Run() {
	for {
		select {
		case client := <-h.register:
			h.clients[client] = true
		case client := <-h.unregister:
			if _, ok := h.clients[client]; ok {
				delete(h.clients, client)
			}
		case plan := <-h.broadcast:
			for client := range h.clients {
				client.WriteTextMessage(plan)
			}
		}
	}
}

type WebSocketClient struct {
	hub *Hub

	// The websocket connection.
	conn *websocket.Conn
	// Buffered channel of outbound messages.
	Send chan []byte
}

func NewWebSocketClient(hub *Hub, conn *websocket.Conn) *WebSocketClient {
	return &WebSocketClient{hub: hub, conn: conn, Send: make(chan []byte)}
}
func (wsclient *WebSocketClient) WriteTextMessage(msg []byte) error {
	return wsclient.conn.WriteMessage(websocket.TextMessage, msg)
}
