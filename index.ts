import "reflect-metadata";

type QuipuActions = "create" | "read" | "update" | "delete" | "query" | "stop";

type JsonSchema = {
	title: string;
	description?: string;
	type?: "object" | "array" | "string" | "number" | "integer" | "boolean" | "null";
	properties: {
		[key: string]: any;
	};
	required?: string[];
	enum?: any[];
	items?: any;
};

type Status = {
	code: number;
	message: string;
	id?: string;
	definition?: JsonSchema;
};

type CollectionMetadataType = {
	id: string;
	name: string;
};

type CollectionType = {
	id: string;
	name: string;
	schema: JsonSchema;
};

type ActionRequest = {
	event: QuipuActions;
	id?: string | null;
	data?: object | null;
};

type SSEEvent<T> = {
	data: T;
	event: "create" | "read" | "update" | "delete" | "query" | "stop";
};

const isObject = (value: any): value is object => {
	return value && typeof value === "object" && !Array.isArray(value);
};

const useStream = async <T>(
	url: string,
	data: T,
	callback: (data: string) => any,
	options?: RequestInit,
): Promise<void> => {
	const response = await fetch(url, {
		...options,
		method: "POST",
		body: JSON.stringify(data),
		headers: { "Content-Type": "application/json" },
	});

	if (!response.body) {
		throw new Error("No response body");
	}

	const reader = response.body.getReader();
	const decoder = new TextDecoder();
	let buffer = "";

	while (true) {
		const { done, value } = await reader.read();
		if (done) {
			break;
		}

		buffer += decoder.decode(value, { stream: true });
		let lines = buffer.split("\n");

		for (let i = 0; i < lines.length - 1; i++) {
			const line = lines[i].replace(/^data: /, "").trim();
			if (line && line !== "[DONE]") {
				callback(line + "\n");
			}
		}

		buffer = lines[lines.length - 1];
	}

	if (buffer) {
		const line = buffer.replace(/^data: /, "").trim();
		if (line && line !== "[DONE]") {
			callback(line + "\n");
		}
	}
};

function jsonSchemaGenerator(typeName: string) {
	return function (target: any, key: string, descriptor: PropertyDescriptor) {
		const originalMethod = descriptor.value;
		const jsonSchema: JsonSchema = {
			title: `${typeName}:${target.collectionId}`,
			type: "object",
			properties: {},
			required: [],
		};

		const generateSchema = (value: any): JsonSchema => {
			if (Array.isArray(value)) {
				return {
					type: "array",
					items: generateSchema(value[0]),
					properties: {},
				};
			} else if (isObject(value)) {
				const nestedSchema: JsonSchema = {
					title: Object.keys(value)[0],
					type: "object",
					properties: {},
					required: [],
				};
				for (const key in value) {
					nestedSchema.properties[key] = generateSchema(value[key]);
					if (value[key] !== null) {
						nestedSchema.required.push(key);
					}
				}
				return nestedSchema;
			} else {
				if (typeof value === "object") {
					return {
						type: "object",
						properties: {},
						required: [],
					};
				} else {
					return {
						type: typeof value as "string" | "number" | "boolean" | "null",
						properties: {},
					};
				}
			}
		};

		descriptor.value = function (...args: any[]) {
			const result = originalMethod.apply(this, args);
			const keys = Object.keys(result);

			keys.forEach((key) => {
				jsonSchema.properties[key] = generateSchema(result[key]);
				if (result[key] !== null) {
					jsonSchema.required.push(key);
				}
			});

			return jsonSchema;
		};
	};
}

interface IQuipuBase<T> {
	baseUrl: string;
	collectionId?: string;
	data?: T;
	id?: string;
	limit?: number;
	offset?: number;
	buildUrl(endpoint: string, id?: string): string;
	fetch(actionRequest: ActionRequest, colId: string, endpoint: string): Promise<Status | T | T[] | CollectionType | CollectionMetadataType>;
	getJsonSchema<T>(data: T): JsonSchema;
}

export class QuipuBase<T> implements IQuipuBase<T> {
	constructor(
		public baseUrl: string = "https://quipubase.online",
	) { }

	@jsonSchemaGenerator("data")
	getJsonSchema<T>(data: T): JsonSchema {
		return data as unknown as JsonSchema;
	}

	async fetch(
		actionRequest: ActionRequest,
		colId: string,
		endpoint: string
	): Promise<Status | T | T[] | CollectionType | CollectionMetadataType> {
		const url = this.buildUrl(endpoint, colId);
		const options = {
			method: "PUT",
			headers: {
				"Content-Type": "application/json",
			},
			body: JSON.stringify(actionRequest),
		};

		const response = await fetch(url, options);
		return await response.json();
	}

	buildUrl(endpoint: string, id?: string): string {
		return `${this.baseUrl}${endpoint}${id ? `/${id}` : ""}`;
	}

	// Collection Management
	async createCollection(schema: JsonSchema, name: string): Promise<CollectionType> {
		const url = this.buildUrl("/v1/collections");
		const collectionData = {
			schema,
			name
		};

		const options = {
			method: "POST",
			headers: {
				"Content-Type": "application/json",
			},
			body: JSON.stringify(collectionData),
		};

		const response = await fetch(url, options);
		return await response.json() as CollectionType;
	}

	async getCollection(collectionId: string): Promise<CollectionType> {
		const url = this.buildUrl("/v1/collections", collectionId);
		const response = await fetch(url);
		return await response.json() as CollectionType;
	}

	async deleteCollection(collectionId: string): Promise<Record<string, boolean>> {
		const url = this.buildUrl("/v1/collections", collectionId);
		const options = {
			method: "DELETE",
			headers: {
				"Content-Type": "application/json",
			},
		};

		const response = await fetch(url, options);
		return await response.json() as Record<string, boolean>;
	}

	async listCollections(limit: number = 100, offset: number = 0): Promise<CollectionMetadataType[]> {
		const params = new URLSearchParams();
		params.set("limit", limit.toString());
		params.set("offset", offset.toString());

		const url = `${this.buildUrl("/v1/collections")}?${params.toString()}`;
		const response = await fetch(url);
		return await response.json() as CollectionMetadataType[];
	}

	// Document Operations
	async create(collectionId: string, data: T): Promise<T> {
		const actionRequest: ActionRequest = {
			event: "create",
			data: data || null,
		};

		return await this.fetch(actionRequest, collectionId, "/v1/collections") as T;
	}

	async read(collectionId: string, id: string): Promise<T> {
		const actionRequest: ActionRequest = {
			event: "read",
			id,
		};

		return await this.fetch(actionRequest, collectionId, "/v1/collections") as T;
	}

	async update(collectionId: string, id: string, data: Partial<T>): Promise<T> {
		const actionRequest: ActionRequest = {
			event: "update",
			id,
			data,
		};

		return await this.fetch(actionRequest, collectionId, "/v1/collections") as T;
	}

	async delete(collectionId: string, id: string): Promise<Status> {
		const actionRequest: ActionRequest = {
			event: "delete",
			id,
		};

		return await this.fetch(actionRequest, collectionId, "/v1/collections") as Status;
	}

	async query(collectionId: string, data: Partial<T>): Promise<T[]> {
		const actionRequest: ActionRequest = {
			event: "query",
			data,
		};

		return await this.fetch(actionRequest, collectionId, "/v1/collections") as T[];
	}

	// PubSub Operations
	async publishEvent(collectionId: string, actionRequest: ActionRequest): Promise<T> {
		const url = this.buildUrl("/v1/events", collectionId);
		const options = {
			method: "POST",
			headers: {
				"Content-Type": "application/json",
			},
			body: JSON.stringify(actionRequest),
		};

		const response = await fetch(url, options);
		return await response.json();
	}

	// Stream subscription with custom handling
	async subscribeToEvents(collectionId: string, callback: (data: SSEEvent<T>) => any): Promise<() => void> {
		if (typeof window !== 'undefined' && 'EventSource' in window) {
			return this._subscribeToEvents(collectionId, callback);
		}

		const url = this.buildUrl("/v1/events", collectionId);

		// Using fetch with a reader for more control
		const response = await fetch(url);
		const reader = response.body!.getReader();
		const decoder = new TextDecoder();
		let closed = false;

		const processChunks = async () => {
			while (!closed) {
				const { done, value } = await reader.read();
				if (done) break;

				const chunk = decoder.decode(value, { stream: true });
				callback(JSON.parse(chunk));
			}
		};

		processChunks();

		return () => {
			closed = true;
		};
	}

	async _subscribeToEvents(collectionId: string, callback: (event: any) => void): Promise<() => void> {
		const url = this.buildUrl("/v1/events", collectionId);
		const eventSource = new EventSource(url);

		eventSource.onmessage = (event) => {
			callback(JSON.parse(event.data));
		};

		eventSource.onerror = (error) => {
			console.error("EventSource error:", error);
			eventSource.close();
		};

		// Return a function to close the connection
		return () => {
			eventSource.close();
		};
	}
}

export type { CollectionType, CollectionMetadataType, Status, JsonSchema, QuipuActions, ActionRequest };